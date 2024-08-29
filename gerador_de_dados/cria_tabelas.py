from data_generator_pack.auxiliar import *
from data_generator_pack.generator import *
import pandas as pd
import datetime as dt

# cria tabela produtos
if __name__ == "__main__":
    # vestuario
    lista_de_produtos = ["calça jeans","calça sarja","calça moletom","camiseta","camisa"]
    lista_de_tamanhos = ["pp","p","m","g","gg","xg"]
    lista_de_colecoes = ["verao","inverno","7 a 1","corinthias","clube da esquina","bossa"]
    lista_de_modelos = ["f","m","u"]
    lista_de_categoria = ["vestuario"]

    produtos = cria_dicionario_preenchido(\
        produto=lista_de_produtos,
        tamanho=lista_de_tamanhos,
        colecao=lista_de_colecoes,
        modelo=lista_de_modelos,
        categoria = lista_de_categoria)

    # acessorios
    lista_de_produtos_acessorios = ["boné","relogio","chapeu","laço","pulseira"]
    lista_de_tamanhos_acessorios = ["u"]
    lista_de_colecoes_acessorios = ["parque são jorge","ogum","iorubá"]
    lista_de_modelos_acessorios = ["f","m","u"]
    lista_de_categoria = ["acessorios"]

    acessorios = cria_dicionario_preenchido(\
        produto=lista_de_produtos_acessorios,
        tamanho=lista_de_tamanhos_acessorios,
        colecao=lista_de_colecoes_acessorios,
        modelo=lista_de_modelos_acessorios,
        categoria = lista_de_categoria)

    # calçados
    lista_de_produtos_calçados = ["tênis","sapato","chinelo","pantufa","sandália"]
    lista_de_tamanhos_calçados = ["35","36","37","38","39","40","41","42"]
    lista_de_colecoes_calçados = ["bras","sao carlos","botucatu"]
    lista_de_modelos_calçados = ["f","m","u"]
    lista_de_categoria = ["calçados"]

    calcados = cria_dicionario_preenchido(\
        produto=lista_de_produtos_calçados,
        tamanho=lista_de_tamanhos_calçados,
        colecao=lista_de_colecoes_calçados,
        modelo=lista_de_modelos_calçados,
        categoria = lista_de_categoria)

    dicionario1 = cria_dicionario_padronizado(produtos,tamanho_lista=5)

    dicionario2 = cria_dicionario_padronizado(acessorios,tamanho_lista=5)

    dicionario3 = cria_dicionario_padronizado(calcados,tamanho_lista=5)

    data = integra_dicionario(integra_dicionario(dicionario1,dicionario2),dicionario3) 

    data = adiciona_creation_date(data,mode="random")

    dim_produto = cria_chave_produtos(data)

    # dim_produto = adiciona_chave_a_entidade(data,'produto')

    dim_produto = adiciona_float_a_entidade(data=dim_produto,entidade="chave",nome_da_nova_chave="custo",min=1000,max=2000)

    df = pd.DataFrame(dim_produto)
    df = df.drop_duplicates(subset=["produto","tamanho","colecao","modelo","categoria"], keep='first')

    data = pd.DataFrame(data).drop_duplicates(subset=["produto","tamanho","colecao","modelo","categoria"], keep='first')

    cursor, cnx = set_pyodbc_cursor()

    cria_tabelas_sql(cria_sql_ddl(dicionario=dim_produto,tipo="create",nome_tabela="dim_produto"))

    a,b,c = pyodbc_insert_params(dim_produto)

    writes_into_sqlserver(df,"dim_produto",a,b,c)


    # cria tabela clientes
    cursor, cnx = set_pyodbc_cursor()

    clientes = cria_dim_pessoas(tamanho_da_lista=10,distribuicao=0.5)
    ch = ['chave', 'nome', 'idade', 'genero', 'uf', 'cidade', 'dados']
    del clientes["dados"]


    clientes = adiciona_creation_date(clientes,mode="random")

    df_clientes = pd.DataFrame(clientes)

    cria_tabelas_sql(cria_sql_ddl(dicionario=clientes,tipo="create",nome_tabela="dim_clientes"))

    a,b,c = pyodbc_insert_params(clientes)

    writes_into_sqlserver(df_clientes,"dim_clientes",a,b,c)


    # cria tabela vendedores
    cursor, cnx = set_pyodbc_cursor()

    vendedores = cria_dim_pessoas(tamanho_da_lista=4,distribuicao=0.5)
    vendedores = adiciona_creation_date(vendedores,mode="random")


    del vendedores["dados"]
    df_vendedores = pd.DataFrame(vendedores)

    cria_tabelas_sql(cria_sql_ddl(dicionario=clientes,tipo="create",nome_tabela="dim_vendedores"))

    a,b,c = pyodbc_insert_params(vendedores)

    writes_into_sqlserver(df_vendedores,"dim_vendedores",a,b,c)

    # cria tabela vendas
    vendas_dicionario = cria_tabela_vendas(tamanho_lista=10,
                            dim_produto=dim_produto,
                            df_clientes=df_clientes,
                            df_vendedores=df_vendedores)



    a,b,c = pyodbc_insert_params(vendas_dicionario)
    cria_tabelas_sql(cria_sql_ddl(dicionario=vendas_dicionario,tipo="create",nome_tabela="fato_vendas"))
    df_vendas = pd.DataFrame(vendas_dicionario)


    df_vendas['creation_date'] = df_vendas['creation_date'].astype(str)
    df_vendas['creation_date'] = df_vendas['creation_date'].replace('-', '',regex=True)

    writes_into_sqlserver(df_vendas,"fato_vendas",a,b,c)






        
        