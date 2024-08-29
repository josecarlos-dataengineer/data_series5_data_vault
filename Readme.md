## Descrição

Neste repositório é abordada a metodologia/modelagem **Data Vault**. Esta modelagem é indicada para cenários de integração de dados em que uma mesma entidade tenha fontes variadas, desde que tenham uma chave de negócio invariável. Por exemplo a entidade pessoa, que pode vir de várias fontes como arquivos, API, SQL e NOSQL, mas sempre terá o cpf ou rg como chave invariável. A documentação da Databricks sobre Data Vault pode ser uma boa leitura, caso seja o primeiro contato.
!["Data_Vault"](https://www.databricks.com/br/glossary/data-vault). <br>

A arquitetura é semelhante a de um datalakehouse, porém somente até a camada Bronze. A partir da camada Silver, inserimos as tabelas Hub, Satelite e Link.

camada  | finalidade                    | formato
------  | ----------                    | -------
landing | armazenar dados brutos        | json
bronze  | aplicar tipagem de dados      | parquet
silver  | aplicar modelagem Data Vault  | Delta Table

### Requisitos:
Microsoft SQL Server 2019 
Python 3.9

### Status: Em desenvolvimento

etapa                           | estado                    
------                          | ----------                   
API                             | Completa    
Landing                         | Completa   
Bronze                          | Completa  
Silver                          | Completa
Gold OBT                        | Completa
Revisar nomes das colunas OBT   | Pendente  
Gold Star Schema                | Pendente 
Integração de novas fontes      | Pendente
Análise Power BI                | Pendente

## Objetivo
Coletar os dados disponíveis na API, transformálos, intregrá-los e disponibilizá-los em tabela Delta OBT e também Star Schema.

## Modelagem Data Vault

## Gerador de Dados

## API Flask

## Prcesso de criação e atualização dos dados

Acesse a pasta em que o projeto foi clonado.

Primeiro passo é criar um ambiente virtual, você pode criá-lo da maneira que estiver mais confortável. Como sugestão, pode usar o comando:


```
python -m venv <nome_do_ambiente>
```

Após criar o ambiente, ative-o:

```
data_series_5_venv\Scripts\Activate.ps1
```

Instale as dependências:

```
pip install -r requirements.txt
```

Execute o arquivo <gerador_de_dados\cria_tabelas.py>
Este arquivo criará as tabelas utilizadas e as carregará no SQLSERVER.

```
python gerador_de_dados\cria_tabelas.py
```

Após carregadas as tabelas, execute o arquivo <flask\api.py>

```
python flask\api.py
```

A execução desse arquivo, disponibiliza os dados via API para que sejam consumidos via biblioteca Requests.

Abra um novo terminal e execute <etl\functions.py>

```
cd etl \
python functions.py
```

A execução fará todo o processo de ETL, carregando as respostas da API em formato JSON na camada ***Landing***.
Fará a tipagem de dados e transformaçoes para salvar em formato ***Parquet*** na camada ***Bronze***.
Fará novas transformações para salvar em formato Delta, utilizando a modelagem ***Data Vault*** na camada ***Silver***.
E por fim salvará, ainda em formato Delta Table, as tabelas ***Star Schema*** e ***OBT*** na camada ***Gold***.

Para consumir os dados da camada Gold no Power BI, fica a sugestão de alguns mmétodos:

Executar o arquivo <etl\serving.py>; este arqiuvo cria um serviço API para a Delta Table OBT. Assim os dados podem ser consumidos no Power BI via API.

```
python etl\serving.py
```

Ler os dados, converter para Pandas e Salva em formato csv ou txt.
Ler os dados e carregá-los no banco de dados.

Para a leitura, pode-se utilizar a função reading_gold_obt(), armazenada no arquivo functions.py. Esta função lê a tabela OBT e retorna em formato Pandas Dataframe.