import requests
import json
from io import BytesIO,StringIO
import pandas as pd
from functions_packages.path_builder import path_definition,path_builder
import datetime as dt
from data_auxiliars.data_quality import *
import pyarrow as pa
import pyarrow.parquet as pq
import deltalake as dtl
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import hashlib
from time import sleep

params = {"sources":["dim_clientes","dim_produto","dim_vendedores","fato_vendas"],
          "paths":{"hub":r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_5_data_vault\etl\3_silver\dev\hub",
                   "satelite":r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_5_data_vault\etl\3_silver\dev\satelite",
                   "link":r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_5_data_vault\etl\3_silver\dev\link",
                   "obt_gold":r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_5_data_vault\etl\4_gold\dev\obt"}}





def generate_hash(dataframe, *cols):
    # Concatena os valores das colunas especificadas em cada linha
    def concat_and_hash(row):
        # Concatena os valores das colunas selecionadas
        combined = ''.join(str(row[col]) for col in cols)
        # Gera o hash SHA-256
        return hashlib.sha256(combined.encode()).hexdigest()

    # Aplica a função de hash ao DataFrame e retorna uma nova série
    return dataframe.apply(concat_and_hash, axis=1)

def is_api_available(table_name:str) -> list:
    """
    Args:
        table_name (str): Nome da tabela a qual se consulta via API

    Raises:
        Exception: Indisponibilidade do serviço

    Returns:
        str: lista com mensagem 'Disponível', response e url
    """
    url = f"http://127.0.0.1:5000/api/tabela/{table_name}"
    response = requests.get(url)

    if str(response) == "<Response [200]>":
        return ["disponível",str(response),url]
    else:
        raise Exception(f"Url: {url} indisponível: {response}")

def get_data_from_api(table_name) -> tuple: 
    """_summary_

    Args:
        table_name (_type_): Nome da tabela a qual se consulta via API

    Returns:
        tuple: response.content,table_name
    """
    resp_list = is_api_available(table_name)
    if resp_list[0] == "disponível":
        response = requests.get(resp_list[2])
        # data = json.load(response.content)
    
    return response.content,table_name

def writeson_landing(table_name:str,layer=1,extension="json") -> object:
    """_summary_

    Args:
        table_name (str): Nome da tabela a qual se consulta via API
        layer (int, optional): Camada na qual será escrito o arquivo. Defaults to 1.
        extension (str, optional): Extensão do arquivo. Defaults to "json".
    Calls: 
        get_data_from_api(): Requisita API através do nome da tabela
        path_definition(): Define o diretório conforme table_name,layer e extension
        path_builder(): Cria o diretório, caso não exista

    Writes:
        object: Escreve o objeto no diretorio definido
    """
    
    data, table_name = get_data_from_api(table_name)

    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=layer,
        table_name=table_name,
        src_extension=extension)

    path_builder(src_path)

    with BytesIO(data) as file_like_object:

        # Abre um arquivo local em modo binário para escrita
        with open(src_path, 'wb') as local_file:

            # Escreve o conteúdo do BytesIO no arquivo local
            local_file.write(file_like_object.getbuffer())



def read_from_landing(table_name:str,layer=1,extension="json") -> object:
    """_summary_

    Args:
        table_name (str): Nome da tabela a qual se consulta via API
        layer (int, optional): Camada na qual será escrito o arquivo. Defaults to 1.
        extension (str, optional): Extensão do arquivo. Defaults to "json".

    Returns:
        object: Pandas DataFrame
    """

    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=layer,
        table_name=table_name,
        src_extension=extension)    

    with open(src_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        df = pd.DataFrame(data)

        return df    
    
def add_columns(df:pd.DataFrame,table_name:str) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): Pandas Dataframe a ser tratado
        table_name (str): Nome da tabela fonte do Dataframe

    Returns:
        pd.DataFrame: Pandas Dataframe com a nova coluna 'load_date'
    """
    df["load_date"] = dt.datetime.now()
    return df 
    

def writeson_bronze(table_name:str,layer=2,extension="parquet") -> object:
    """
    Args:
        table_name (str): Nome da tabela a ser coletada e escrita
        layer (int, optional): Camada na qual será escrito o arquivo. Defaults to 2.
        extension (str, optional): Extensão do arquivo. Defaults to "csv".

    Writes:
        object: csv file
    
    example: 
        writeson_bronze(table_name="vendedores")
    """
    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        dstn_layer=layer,
        table_name=table_name,
        dstn_extension=extension)
    
    path_builder(dstn_path)
    
    df = read_from_landing(table_name=table_name) 
    df = add_columns(df,table_name)
    df = converte_tipos_de_dados(df)
    # df.drop(columns="table_id",inplace=True)
    schema_ = schemas(dataframe=df,mode="pyarrow")  
    
    table = pa.Table.from_pandas(df,schema=define_schema_pyarrow(dicionario=schema_))

    pq.write_table(table,dstn_path)
  
table_name = "dim_clientes"

def hub_reading(table_name:str,layer=2,extension="parquet"):
    """_summary_

    Args:
        table_name (str): _description_
        layer (int, optional): _description_. Defaults to 2.
        extension (str, optional): _description_. Defaults to "parquet".

    Returns:
        _type_: _description_
        
    Example:
        hub_reading("fato_vendas")
    """

    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=layer,
        table_name=table_name,
        src_extension=extension)
    
    df = pd.read_parquet(src_path)
 

    df["hub_date"] = df["creation_date"]
    df["source"] = table_name
    df["hashkey"] = df["chave"]
    df = df[["hashkey","hub_date","source"]]
    
    return df

def hub_writting(table_name:str,dstn_layer=3):
    """_summary_

    Args:
        table_name (str): _description_
        dstn_layer (int, optional): _description_. Defaults to 3.
        
    Example:
        hub_writting(table_name="dim_vendedores") 
    """

    src_path, src_extension,dstn_path = path_definition(
        dstn_layer=dstn_layer,
        table_name=table_name,
        mode="delta"
        )
    
    df = hub_reading(table_name)

    dstn_path = f"{dstn_path}\\hub\\{table_name}"
    write_deltalake(dstn_path,df,mode="append")
# -------------------------------------------------------
# LINKS
def link_reading(table_name:str,layer=2,extension="parquet"):
    """_summary_

    Args:
        table_name (str): _description_
        layer (int, optional): _description_. Defaults to 2.
        extension (str, optional): _description_. Defaults to "parquet".

    Returns:
        _type_: _description_
        
    Example:
        hub_reading("dim_clientes")
    """

    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=layer,
        table_name=table_name,
        src_extension=extension)
    
    df = pd.read_parquet(src_path)
 
    df["hashkey_vendas"] = df["chave"]
    df["hashkey_cliente"] = df["id_cliente"]
    df["hashkey_produto"] = df["id_produto"]
    df["hashkey_vendedor"] = df["id_vendedor"] 
    df["hub_date"] = df["creation_date"]
    df["hashkey"] = generate_hash(df,"hashkey_vendas","hashkey_cliente","hashkey_produto","hashkey_vendedor")
    df = df[["hashkey_vendas","hashkey_cliente","hashkey_produto","hashkey_vendedor","hub_date","hashkey"]]
    
    return df

def link_writting(table_name:str,dstn_layer=3):
    
    """_summary_

    Args:
        table_name (str): _description_
        dstn_layer (int, optional): _description_. Defaults to 3.
        
    Example:
        link_writting(table_name="fato_vendas") 
    """

    src_path, src_extension,dstn_path = path_definition(
        dstn_layer=dstn_layer,
        table_name=table_name,
        mode="delta"
        )
    df = link_reading(table_name)
    dstn_path = f"{dstn_path}\\link\\{table_name}"
    write_deltalake(dstn_path,df,mode="append")
    
# -------------------------------------------------------
# SATELITE
def satelite_reading(table_name:str,layer=2,extension="parquet"):
    """_summary_

    Args:
        table_name (str): _description_
        layer (int, optional): _description_. Defaults to 2.
        extension (str, optional): _description_. Defaults to "parquet".

    Returns:
        _type_: _description_
        
    Example:
        satelite_reading("dim_clientes")
    """

    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=layer,
        table_name=table_name,
        src_extension=extension)
    
    df = pd.read_parquet(src_path)
 
 
    df["hub_date"] = df["creation_date"]
    df["source"] = table_name
    df["hashkey"] = df["chave"]
    df.drop(columns=["creation_date","chave"])
    
    return df

def satelite_writting(table_name:str,dstn_layer=3):
    """_summary_

    Args:
        table_name (str): _description_
        dstn_layer (int, optional): _description_. Defaults to 3.
        
    Example:
        satelite_writting(table_name="dim_vendedores") 
    """

    src_path, src_extension,dstn_path = path_definition(
        dstn_layer=dstn_layer,
        table_name=table_name,
        mode="delta"
        )
    
    df = satelite_reading(table_name)

    dstn_path = f"{dstn_path}\\satelite\\{table_name}"
    write_deltalake(dstn_path,df,mode="append")

def writeson_silver(table_name:str):    

    hub_writting(table_name=table_name)
    sleep(2)
    satelite_writting(table_name=table_name) 
    sleep(5)
    
    link_writting(table_name=table_name) 


def reading_vault() -> object:
    
    hub = params["paths"]["hub"]
    satelite = params["paths"]["satelite"]
    link = params["paths"]["link"]

    clientes = "\\dim_clientes"
    produtos = "\\dim_produto"
    vendedores = "\\dim_vendedores"
    vendas = "\\fato_vendas"

    dt_clientes_hub = DeltaTable(hub + clientes)
    dt_produtos_hub = DeltaTable(hub + produtos)
    dt_vendedores_hub = DeltaTable(hub + vendedores)
    dt_vendas_hub = DeltaTable(hub + vendas)

    dt_clientes_sat = DeltaTable(satelite + clientes)
    dt_produtos_sat = DeltaTable(satelite + produtos)
    dt_vendedores_sat = DeltaTable(satelite + vendedores)
    dt_vendas_sat = DeltaTable(satelite + vendas)

    dt_link = DeltaTable(link + vendas)

    df_link = dt_link.to_pandas()

    df_clientes_hub = dt_clientes_hub.to_pandas()
    df_produtos_hub = dt_produtos_hub.to_pandas()
    df_vendedores_hub = dt_vendedores_hub.to_pandas()
    df_vendas_hub = dt_vendas_hub.to_pandas()

    df_clientes_sat = dt_clientes_sat.to_pandas()
    df_produtos_sat = dt_produtos_sat.to_pandas()
    df_vendedores_sat = dt_vendedores_sat.to_pandas()
    df_vendas_sat = dt_vendas_sat.to_pandas()

    clientes = pd.merge(
        df_clientes_hub,
        df_clientes_sat,
        on="hashkey",
        how="inner")

    produtos = pd.merge(
        df_produtos_hub,
        df_produtos_sat,
        on="hashkey",
        how="inner")

    vendedores = pd.merge(
        df_vendedores_hub,
        df_vendedores_sat,
        on="hashkey",
        how="inner")

    vendedores = vendedores.drop(columns=['hub_date_x','source_x','chave','hub_date_y'])
    produtos = produtos.drop(columns=['hub_date_x','source_x','chave','hub_date_y'])
    clientes = clientes.drop(columns=['hub_date_x','source_x','chave','hub_date_y'])

    return clientes, produtos, vendedores, df_link

def merges_vault_tables() -> object:
    
    clientes, produtos, vendedores, df_link = reading_vault()
    
    datavault = df_link.merge(clientes, 
                            left_on="hashkey_cliente",
                            right_on="hashkey",
                            how="inner")\
                        .merge(produtos,
                                left_on="hashkey_produto",
                                right_on="hashkey",
                                how="inner")

    datavault = datavault.drop(columns=["source_y_y","load_date_y",
                                        "table_id_y","creation_date_y",
                                        "source_y_x","load_date_x",
                                        "table_id_x","creation_date_x",
                                        "hashkey_y","hashkey_x","hashkey"])                
                        
    datavault = datavault.merge(vendedores,
                            left_on="hashkey_vendedor",
                            right_on="hashkey",
                            how="inner")
                        
                        
    # datavault.drop_duplicates(subset=["hub_date"],keep="first")                    

    return datavault


def writeson_gold():
    
    obt_dataframe = merges_vault_tables()
    obt_dataframe["loaded_date_gold_obt"] = dt.datetime.now()
    
    src_path, src_extension,dstn_path = path_definition(
        src_layer=3,
        dstn_layer=4,

        mode="delta"
        )

    write_deltalake(dstn_path + r"\\obt",obt_dataframe,mode="append")
    
    return

def reading_gold_obt():
    """_summary_

    Returns:
        _type_: _description_
        
    Example:
        reading_gold_obt()
    """
        
    dt = DeltaTable(params["paths"]["obt_gold"])

    df = dt.to_pandas()
    dt.vacuum()
    
    return df

if __name__ == "__main__":    

    for i in params["sources"]:

        writeson_landing(table_name=i)
        writeson_bronze(table_name=i)
        hub_writting(table_name=i)
        satelite_writting(table_name=i) 
    link_writting(table_name="fato_vendas")
    writeson_gold() 
    reading_gold_obt()




