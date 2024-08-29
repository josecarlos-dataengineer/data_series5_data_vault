## Descrição

## Gerador de dados

Este repositório contém funções que facilitam a criação de tabelas para estudos de caso. 
Aqui estão contidas funções para gerar tabelas de pessoas com nomes aleatórios, bem como a criação de entidades aleatórias, facilitando a criação de estudos diversos, sejam eles na área de vendas, rh, finanças ou qualquer área que utiliza tabelas estruturadas com tipagem de dados string. <br>

O arquivo main.py contém o script para a criação de um estudo no contexto de vendas, mas pode ser personalizado para outros cenários.
Ao executar o arquivo, serão criadas e alimentadas três tabelas [dim_clientes, dim_produtos, dim_vendedores].

## DDL das tabelas de exemplo:

```sql
CREATE TABLE [dbo].[dim_clientes](
	[chave] [varchar](100) NULL,
	[nome] [varchar](100) NULL,
	[idade] [varchar](100) NULL,
	[genero] [varchar](100) NULL,
	[uf] [varchar](100) NULL,
	[cidade] [varchar](100) NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[dim_produto](
	[table_id] [varchar](100) NULL,
	[produto] [varchar](100) NULL,
	[tamanho] [varchar](100) NULL,
	[colecao] [varchar](100) NULL,
	[modelo] [varchar](100) NULL,
	[categoria] [varchar](100) NULL,
	[chave] [varchar](100) NULL,
	[custo] [varchar](100) NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[dim_vendedores](
	[chave] [varchar](100) NULL,
	[nome] [varchar](100) NULL,
	[idade] [varchar](100) NULL,
	[genero] [varchar](100) NULL,
	[uf] [varchar](100) NULL,
	[cidade] [varchar](100) NULL
) ON [PRIMARY]
```


### Versão do SQL:
```sql
select @@version
```

***Microsoft SQL Server 2019 (RTM) - 15.0.2000.5 (X64)   Sep 24 2019 13:48:23   Copyright (C) 2019 Microsoft Corporation  Developer Edition (64-bit) on Windows 10 Home 10.0 <X64> (Build 19045: ) (Hypervisor)***



O tamanho de cada tabela é ajustável, através do ajuste dos argumentos das funções.

No exemplo abaixo, está sendo criada uma tabela com 10 linhas, sendo femininos 60% dos nomes

```python
cria_dim_pessoas(tamanho_da_lista=10,distribuicao=0.6)
```

Abaixo,um outro exemplo de aplicação. Nesse caso está sendo criado um dicionário com calçados, e cada chave tem respectivamente uma lista de 50 valores escolhidos aleatóriamente dentre os elementos de cada lista. E uma vez que todas os valores de chave contém o mesmonúmero de elementos, pode-se aplicar pd.DataFrame para obter um dataframe do dicionário.

```python
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

dicionario_calçados = cria_dicionario_padronizado(calcados,tamanho_lista=50)
```

## Criação das tabelas.

A criação e inserção de dados nas tabelas SQLSERVER seguem um padrão indicado na documentação da Microsoft !["documentação"](https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-sql-server?view=sql-server-ver16#load-a-dataframe-from-the-csv-file).

exemplo:
```python
def writes_into_sqlserver(dataframe:pd.DataFrame,tabela,cursor:object,cnxn:object,columns_names:str,question_marks:str,columns:str):
     sleep(5)
     for index, row in dataframe.iterrows():
          
          exec(f'cursor.execute(f"INSERT INTO dbo.{tabela} {columns_names} values{question_marks}", {columns})')
     cnxn.commit()
     cursor.close()
```

A função acima recebe argumentos que são retornados como resultado de outras funções. Haverá uma outra seção com a explicação detalhada.

## logs
A pasta data_packs\ddl_logs armazena arquivos do tipo json contendo um pequeno log de criação de cada tabela. 

exemplo:
```json
{
    "data_criacao": "2024-08-19 01:09:44.991500",
    "tipo": "create",
    "nome": "dim_clientes",
    "query": "CREATE TABLE dim_clientes (chave VARCHAR(100),nome VARCHAR(100),idade VARCHAR(100),genero VARCHAR(100),uf VARCHAR(100),cidade VARCHAR(100),)",
    "drop": "DROP TABLE dim_clientes"
}
```

## Limitações de uso:
Ainda falta ajustar a função que gera o CREATE TABLE, para que se possa criar colunas de tipos diferentes de string. Até o momento, todas as colunas são criadas como VARCHAR(100), vale ter atenção para ajustar para tamanhos maiores, caso deseje.


```python
def cria_sql_ddl(dicionario:dict,tipo="create",nome_tabela="tabela_exemplo"):
    string = ""
    if tipo == "create":
        
        for n in dicionario.keys():
            string = string.__add__(f"{n} VARCHAR(100),")

        string

        query = f"CREATE TABLE {nome_tabela} ({string})".rstrip(",")
        drop = f"DROP TABLE {nome_tabela}"
        with open(DDL_PATH+"\\"+nome_tabela+".json",mode="w+") as file:
            data = str(dt.datetime.now())
            json_file = {"data_criacao":data,"tipo":tipo,"nome":nome_tabela,"query":query,"drop":drop}
            json_file = json.dumps(json_file,indent=4)
            
            file.write(json_file)
            # file.write(f"{tipo},{nome_tabela},{query},{drop}")
    
    return query
```