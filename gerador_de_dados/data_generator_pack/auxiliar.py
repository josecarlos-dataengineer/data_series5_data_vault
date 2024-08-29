import random
import uuid
import pandas as pd
import datetime as dt
from datetime import datetime
import json
import datetime

DDL_PATH = r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_5_data_vault\gerador_de_dados\data_generator_pack\ddl_logs"

def gerador_de_nomes(tamanho_da_lista=10,distribuicao=0.5) -> list:
    """_summary_

    Args:
        tamanho_da_lista (int, optional): _description_. Defaults to 10.
        distribuicao (float, optional): _description_. Defaults to 0.5.

    Returns:
        list: _description_
        
    Example:
        gerador_de_nomes(tamanho_da_lista=101,distribuicao=0.9)

    """
    
    nomes_masculinos = {
        "primeiro_nome": [
            "Lucas", "Gabriel", "Mateus", "Felipe", "Rafael", 
            "Thiago", "João", "Pedro", "Gustavo", "Rodrigo"
        ],
        "segundo_nome": [
            "André", "Silva", "Costa", "Almeida", "Pereira", 
            "Gomes", "Oliveira", "Souza", "Freitas", "Rocha"
        ],
        "sobrenome": [
            "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", 
            "Ferreira", "Almeida", "Pereira", "Costa", "Nascimento"
        ]
    }

    nomes_femininos = {
        "primeiro_nome": [
            "Mariana", "Ana", "Juliana", "Carolina", "Camila", 
            "Isabela", "Fernanda", "Bruna", "Patrícia", "Larissa"
        ],
        "segundo_nome": [
            "Fernanda", "Silva", "Costa", "Almeida", "Pereira", 
            "Gomes", "Oliveira", "Souza", "Freitas", "Rocha"
        ],
        "sobrenome": [
            "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", 
            "Ferreira", "Almeida", "Pereira", "Costa", "Nascimento"
        ]
    }
    n_nomes_femininos = tamanho_da_lista * distribuicao
    n_nomes_femininos = int(round(n_nomes_femininos,0))
    
    n_nomes_masculinos = tamanho_da_lista - n_nomes_femininos
    n_nomes_masculinos = int(round(n_nomes_masculinos,0)) 
      
    lista_f = []
    for n in range(n_nomes_femininos):
        nome = f'{nomes_femininos["primeiro_nome"][random.randint(0,9)]} {nomes_femininos["segundo_nome"][random.randint(0,9)]} {nomes_femininos["sobrenome"][random.randint(0,9)]}'
        lista_f.append(nome)
        
    lista_m = []
    for n in range(n_nomes_masculinos):
        nome = f'{nomes_masculinos["primeiro_nome"][random.randint(0,9)]} {nomes_masculinos["segundo_nome"][random.randint(0,9)]} {nomes_masculinos["sobrenome"][random.randint(0,9)]}'
        lista_m.append(nome)
        
    output = {"m":lista_m,
              "f":lista_f}
        
    return output


def cria_dicionario_vazio(*args) -> dict:
    """_summary_
    
    Args:
        *args: Nomes das chaves

    Returns:
        dict: Dicionario vazio com as chaves descritas
        
    Example:
        cria_dicionario_vazio('a','b'):
            Criará um dicionário com as chaves a e b
    """
    dct = {}    

    for arg in args:
        dct[arg] = []
    return dct

def cria_dicionario_preenchido(**kwargs):
    """_summary_
    Kwargs:
        *kwargs: Nomes das chaves e lista de valores

    Returns:
        _type_: Dicionário preenchido com as chaves e valores passados
        
    Example:
        cria_dicionario_preenchido(e=[1,2],a=[2,3])
    """
    dct = dict()
    for key, value in kwargs.items():
        dct[key] = value
    return dct


def gerador_geografico() -> dict:
    """_summary_

    Returns:
        dict: Dicionario com 10 municipios dos estados SP, MG e RJ
        suas latitudes, longitudes e população.
        
    Example:
        gerador_geografico()
    """
    
    dct = {
        "SP": {
            "São Paulo": [-23.5489, -46.6388, 12.325],
            "Campinas": [-22.9099, -47.0626, 1.223],
            "Santos": [-23.9608, -46.3339, 0.434],
            "Ribeirão Preto": [-21.1782, -47.8103, 0.711],
            "São José dos Campos": [-23.1896, -45.8841, 0.737],
            "Sorocaba": [-23.5015, -47.4521, 0.700],
            "Bauru": [-22.3145, -49.0586, 0.379],
            "Mogi das Cruzes": [-23.5207, -46.1854, 0.460],
            "Piracicaba": [-22.7253, -47.6492, 0.410],
            "Jundiaí": [-23.1857, -46.8978, 0.423]
        },
        "MG": {
            "Belo Horizonte": [-19.9167, -43.9345, 2.512],
            "Uberlândia": [-18.9186, -48.2772, 0.711],
            "Contagem": [-19.9386, -44.0539, 0.673],
            "Juiz de Fora": [-21.7595, -43.3398, 0.573],
            "Betim": [-19.9677, -44.1982, 0.439],
            "Montes Claros": [-16.7283, -43.8578, 0.417],
            "Uberaba": [-19.7471, -47.9392, 0.337],
            "Divinópolis": [-20.1453, -44.8909, 0.241],
            "Governador Valadares": [-18.8531, -41.9418, 0.283],
            "Ipatinga": [-19.4693, -42.5476, 0.265]
        },
        "RJ": {
            "Rio de Janeiro": [-22.9068, -43.1729, 6.747],
            "São Gonçalo": [-22.8268, -43.0637, 1.091],
            "Duque de Caxias": [-22.7858, -43.3049, 0.924],
            "Nova Iguaçu": [-22.7559, -43.4601, 0.828],
            "Niterói": [-22.8832, -43.1034, 0.515],
            "Campos dos Goytacazes": [-21.7622, -41.3181, 0.509],
            "Petrópolis": [-22.5112, -43.1779, 0.307],
            "Volta Redonda": [-22.5203, -44.0996, 0.273],
            "Macaé": [-22.3768, -41.7848, 0.261],
            "Cabo Frio": [-22.8894, -42.0285, 0.235]
        }
    }
   

    return dct

def cria_dicionario_pessoas(gerador_de_nomes:callable):
    """_summary_

    Args:
        gerador_de_nomes (callable): _description_

    Returns:
        _type_: _description_
    Example:
        cria_dicionario_pessoas(gerador_de_nomes(tamanho_da_lista=101,distribuicao=0.9))

    """
    data = gerador_de_nomes
    data_dict = {}
    

    id_list = []
    nome_list = []
    idade_list = []
    genero_list = []

    for k in data.keys():
        for v in data[k]: 
            
            id_list.append(uuid.uuid4().hex[:16])
            nome_list.append(v)
            idade_list.append(random.randint(18,99))
            genero_list.append(k)              
            
            data_dict["chave"] = id_list
            data_dict["nome"] = nome_list
            data_dict["idade"] = idade_list
            data_dict["genero"] = genero_list
            
    return data_dict



       
def atualiza_dicionario_geografico(gerador_geografico:callable):
    """_summary_

    Args:
        gerador_geografico (callable): _description_

    Returns:
        _type_: _description_
    Example:
        atualiza_dicionario_geografico(gerador_geografico())
    """
    geografia = {}
    list_estado = []
    list_dados = []
    list_cidade = []
    data = gerador_geografico
    for estado in data.items():  

        for k,v in estado[1].items():
            
            list_estado.append(estado[0])
            list_cidade.append(k)
            list_dados.append(v)        
            
            geografia["uf"] = list_estado
            geografia["cidade"] = list_cidade
            geografia["dados"] = list_dados
    return geografia



def cria_dim_pessoas(tamanho_da_lista=101,distribuicao=0.9):
    """_summary_

    Args:
        tamanho_da_lista (int, optional): _description_. Defaults to 101.
        distribuicao (float, optional): _description_. Defaults to 0.9.

    Returns:
        _type_: _description_
        
    Example:
        cria_dim_pessoas(tamanho_da_lista=1000)
    """
    geografia = atualiza_dicionario_geografico(gerador_geografico())
    data_dict = cria_dicionario_pessoas(gerador_de_nomes(tamanho_da_lista=tamanho_da_lista,distribuicao=distribuicao))

    list_table_id = []
    list_estado = []
    list_dados = []
    list_cidade = []
    for x in data_dict["chave"]:
        n = random.randint(0,len(geografia["uf"])-1)
        
        list_table_id.append(uuid.uuid4().hex[:16])
        list_estado.append(geografia["uf"][n])
        list_cidade.append(geografia["cidade"][n])
        list_dados.append(geografia["dados"][n])   
    
    data_dict["table_id"] = list_table_id
    data_dict["uf"] = list_estado
    data_dict["cidade"] = list_cidade
    data_dict["dados"] = list_dados
    
    return data_dict



def cria_sql_ddl(dicionario:dict,tipo="create",nome_tabela="tabela_exemplo"):
    string = ""
    if tipo == "create":
        
        for n in dicionario.keys():
            string = string.__add__(f"{n} VARCHAR(200),")

        string
        # query = f"CREATE TABLE IF  {nome_tabela} ({string})".rstrip(",")
        query = f"if not exists (select * from sysobjects where name='{nome_tabela}' and xtype='U') CREATE TABLE  {nome_tabela} ({string})".rstrip(",")
        drop = f"DROP TABLE {nome_tabela}"
        with open(DDL_PATH+"\\"+nome_tabela+".json",mode="w+") as file:
            data = str(dt.datetime.now())
            json_file = {"data_criacao":data,"tipo":tipo,"nome":nome_tabela,"query":query,"drop":drop}
            json_file = json.dumps(json_file,indent=4)
            
            file.write(json_file)
            # file.write(f"{tipo},{nome_tabela},{query},{drop}")
    
    return query



def cria_dicionario_padronizado(dicionario_a_criar,tamanho_lista=100):
    """_summary_

    Args:
        dicionario_a_criar (_type_): _description_
        tamanho_lista (int, optional): _description_. Defaults to 100.

    Returns:
        _type_: _description_
        
    Example:
        cria_dicionario_padronizado(produtos,tamanho_lista=10)
    """
    dicionario_vazio = {}
    for key,value in dicionario_a_criar.items():

        lista_values = []
        lista_ids = []
        lista_creation_date = []
        
        today = str(dt.datetime.now())
        month = int(random.randint(1,12))
        day = int(random.randint(1,28))
        random_ = str(datetime.datetime(2023,month,day))
        
        creation_date_mode = {"today":today,
                              "random":random_}
        
        for n in range(tamanho_lista):
            
            lista_values.append(value[random.randint(0,len(value) - 1)])
            lista_ids.append(uuid.uuid4().hex[:16])
            lista_creation_date.append(creation_date_mode["random"])
            dicionario_vazio["table_id"] = lista_ids
            dicionario_vazio["creation_date"] = lista_creation_date
            dicionario_vazio[key] = lista_values
    
    return dicionario_vazio



def integra_dicionario(dicionario1:dict,dicionario2:dict):
    """_summary_

    Args:
        dicionario1 (dict): _description_
        dicionario2 (dict): _description_
    """


    for k, v in dicionario1.items():
        for n in range(len(dicionario2[k])):
            dicionario1[k].append(dicionario2[k][n])
            
    return dicionario1
 

def adiciona_chave_a_entidade(data:dict,entidade:str):
    # Dicionário para armazenar as chaves já geradas
    keys_dict = {}

    # Lista para armazenar as novas chaves
    keys = []

    for produto in data[entidade]:
        if produto not in keys_dict:
            # Gera uma chave única para o produto
            keys_dict[produto] = uuid.uuid4().hex[:16]
        # Adiciona a chave correspondente ao produto na lista
        keys.append(keys_dict[produto])

    # Adiciona a nova chave ao dicionário original
    data['chave'] = keys

    return data   

def adiciona_float_a_entidade(data:dict,entidade:str,nome_da_nova_chave:str,min:int,max:int):
    # Dicionário para armazenar as chaves já geradas
    keys_dict = {}

    # Lista para armazenar as novas chaves
    keys = []

    for produto in data[entidade]:
        if produto not in keys_dict:
            # Gera uma chave única para o produto
            keys_dict[produto] = random.randint(min,max) / 10
        # Adiciona a chave correspondente ao produto na lista
        keys.append(keys_dict[produto])

    # Adiciona a nova chave ao dicionário original
    data[nome_da_nova_chave] = keys

    return data  

def cria_tabela_vendas(tamanho_lista:int,dim_produto:pd.DataFrame,df_clientes:pd.DataFrame,df_vendedores:pd.DataFrame):
    vendas_dict = {}

    lista_table_id = []
    lista_id_venda = []
    lista_id_produto = []
    lista_id_cliente = []
    lista_id_vendedor = []
    lista_id_quantidade = []
    lista_data_venda = []
    lista_lucro = []
    lista_comissao_negociada = [] 
    
    for n in range(tamanho_lista):
        
        chaves_produtos = list(dim_produto["chave"])
        chaves_clientes = list(df_clientes["chave"])
        chaves_vendedores = list(df_vendedores["chave"])

        n_produtos = random.randint(0,len(chaves_produtos) -1)
        n_clientes = random.randint(0,len(chaves_clientes) -1)
        n_vendedores = random.randint(0,len(chaves_vendedores) -1)

        chaves_produtos[n_produtos]
        chaves_clientes[n_clientes]
        chaves_vendedores[n_vendedores]    
        
        lista_table_id.append(uuid.uuid4().hex[:16])
        lista_id_venda.append(uuid.uuid4().hex[:16])
        lista_id_produto.append(chaves_produtos[n_produtos])
        lista_id_cliente.append(chaves_clientes[n_clientes])
        lista_id_vendedor.append(chaves_vendedores[n_vendedores])
        lista_id_quantidade.append(random.randint(1,4))
        lista_data_venda.append(datetime.datetime.now().date())
        lista_lucro.append(random.randint(50,100)/100)
        lista_comissao_negociada.append(random.randint(3,15)/100)

    vendas_dict = {"table_id":lista_table_id,
                "chave":lista_id_venda,
                "id_produto":lista_id_produto,
                "id_cliente":lista_id_cliente,
                "id_vendedor":lista_id_vendedor,
                "quantidade":lista_id_quantidade,
                "creation_date":lista_data_venda,
                "lucro":lista_lucro,
                "comissao":lista_comissao_negociada}
    
    return vendas_dict

def cria_chave_produtos(dicionario):
    # Criar uma lista para armazenar os UUIDs gerados
    uuids = []
    
    # Iterar por cada índice no dicionário
    for i in range(len(dicionario['produto'])):
        # Combinar os valores de 'produto', 'modelo', 'tamanho' e 'colecao'
        chave_combinada = f"{dicionario['produto'][i]}_{dicionario['tamanho'][i]}_{dicionario['colecao'][i]}_{dicionario['modelo'][i]}"
        
        # Gerar um UUID baseado na combinação
        chave_unica = uuid.uuid5(uuid.NAMESPACE_DNS, chave_combinada).hex
        
        # Adicionar o UUID gerado à lista
        uuids.append(chave_unica)
    
    # Retornar o dicionário com os UUIDs
    dicionario['chave'] = uuids
    return dicionario

def adiciona_creation_date(dicionario:dict,mode="today"):

    lista_creation_date = []

    for v in dicionario["table_id"]:
        
        today = str(dt.datetime.now())
        month = int(random.randint(1,12))
        day = int(random.randint(1,28))
        random_ = str(datetime.datetime(2023,month,day))

        creation_date_mode = {"today":today,
                              "random":random_}
        lista_creation_date.append(creation_date_mode[mode])
        
    dicionario["creation_date"] = lista_creation_date
    return dicionario