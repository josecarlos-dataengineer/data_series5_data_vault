import pyarrow as pa
import pandas as pd

df = pd.DataFrame({"nome":["jose","carla","maria"],"idade":[18,"20",25]})

def schemas(dataframe:pd.DataFrame,mode="pyarrow") -> object:
    """_summary_
    Função detecta nomes de colunas e atribui tipo de dado de acordo com
    convenção definida no dicionário schema.

    Args:
        dataframe (pd.DataFrame): Dataframe que terá as colunas verificadas
        mode (str, optional): Define o framework da tipagem de dados
    Returns:
        _type_: Dicionário com tipos de dados
    """
    
    schema = {"datetime64[ns]":["data","date","creation_date","data_venda","load_date"],
    "float64":["custo","lucro","margem_lucro","preco","comissao_negociada"],
    "int64":["idade","quantidade","volume"]}

    schema_pa = {"object":pa.string(),
    "datetime64[ns]":pa.timestamp('us'),
    "int64":pa.int64(),
    "float64":pa.float64()}

    table_schema_pd = {}
    columns_list = list(dataframe.columns)

    table_schema_pd = {
        column: (
            "datetime64[ns]" if column in schema["datetime64[ns]"] else
            "float64" if column in schema["float64"] else
            "int64" if column in schema["int64"] else
            "object"
        )
        for column in columns_list
    }

    table_schema_pa = {}
    table_schema_pa = {
        column: (
            "pa.timestamp('us')" if column in schema["datetime64[ns]"] else
            "pa.float64()" if column in schema["float64"] else
            "pa.int64()" if column in schema["int64"] else
            "pa.string()"
        )
        for column in columns_list
    }
    output = {"pandas":table_schema_pd,
     "pyarrow":table_schema_pa}   
    
    return output[mode]


def define_schema_pyarrow(dicionario:dict):
    
    list_schemas = []
    
    for k, v in dicionario.items():
        # Avalia a string como código Python para obter o tipo de dado
        data_type = eval(v)  
        # Adiciona uma tupla contendo o nome da coluna e o tipo de dado
        list_schemas.append((k, data_type))
    
    schema_pyarrow = pa.schema(list_schemas)
    
    return schema_pyarrow



def converte_tipos_de_dados(dataframe: pd.DataFrame) -> pd.DataFrame:
    for i in dataframe.columns:

        if dataframe[i].name in ["load_date", "data_venda", "creation_date"]:
            dataframe[i] = pd.to_datetime(dataframe[i], errors='coerce')  # Corrige o uso do pd.to_datetime diretamente

        elif dataframe[i].name in ["idade", "quantidade"]:
            dataframe[i] = pd.to_numeric(dataframe[i], errors='coerce')  # Corrige o uso do pd.to_numeric
            dataframe[i] = dataframe[i].astype("int64")

        elif dataframe[i].name in ["custo", "preco", "lucro", "margem","comissao_negociada"]:
            dataframe[i] = pd.to_numeric(dataframe[i], errors='coerce')  # Corrige o uso do pd.to_numeric
            dataframe[i] = dataframe[i].astype("float64")

        else:
            dataframe[i] = dataframe[i].astype(object)

    return dataframe  # Corrige o posicionamento do return

