import datetime
from datetime import date
from dotenv import load_dotenv
import json
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import pyodbc
import uuid
from time import sleep
# from auxiliar import * 

table_params = {"rows_limit":100}

# Carrega as variáveis do arquivo .env
# load_dotenv("/workspaces/app/.env")
load_dotenv(r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_5_data_vault\gerador_de_dados\.env")

def set_pyodbc_cursor() -> object:
     
     # Configurações do banco de dados
     server = os.getenv("server")
     database = os.getenv("database")
     username = os.getenv("username_")
     password = os.getenv("password")
     driver = os.getenv("driver")
     
     cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
     
     cursor = cnxn.cursor()
     
     return cursor, cnxn

def cria_tabelas_sql(query:str):
     cursor,cnxn  = set_pyodbc_cursor()
     cursor.execute(query)
     cnxn.commit()
     cursor.close()
     return

# data = {"nome":["josé","maria","larissa"],"idade":[52,62,63]}

def pyodbc_insert_params(data_dict:dict) -> list:  
     """
     Args:
          None
          
     Raises:
          None
          
     Returns:
          str: lista com argumentos para 
                      
     Example:
          pyodbc_insert_params(data_dict)   
     """        
     colunas = tuple(data_dict.keys())
     
     param2 = "? " * len(colunas)
     param2 = param2.replace(" ",",").rstrip(",")
     
     param1 = str(tuple(colunas)).replace("'","")
     param2 = str(tuple(param2)).replace("', '","").replace("'","")

     # Transforma a tupla na string desejada
     param3 = ', '.join([f'row.{coluna}' for coluna in colunas])
               
     return [param1, param2, param3]

def writes_into_sqlserver(dataframe:pd.DataFrame,tabela,columns_names:str,question_marks:str,columns:str):
     sleep(1)
     cursor, cnxn = set_pyodbc_cursor()
     if checa_se_tabela_existe(tabela) == 1 and checa_se_tabela_contem_dados(tabela) == False:
          for index, row in dataframe.iterrows():              
               
               exec(f'cursor.execute(f"INSERT INTO dbo.{tabela} {columns_names} values{question_marks}", {columns})')
               cnxn.commit()
          cursor.close()
          print(f"tabela {tabela} inserida no banco de dados")
     print(f"tabela {tabela} já atualizada")

def checa_se_tabela_existe(nome_tabela):
    cursor, cnxn = set_pyodbc_cursor()
    
    query = f"select COUNT(1) from sysobjects where name='{nome_tabela}' and xtype='U'"
    cursor.execute(query)
    
    # Retém o resultado do cursor na variável table_check
    table_check = cursor.fetchone()[0]  # fetchone() retorna uma tupla, então acessamos o primeiro elemento.
    
    cursor.close()
    cnxn.close()  # É importante fechar a conexão quando não for mais necessária.
    
    return table_check

def checa_se_tabela_contem_dados(nome_tabela):
    cursor, cnxn = set_pyodbc_cursor()
    
    query = f"select COUNT(1) from {nome_tabela}"
    cursor.execute(query)
    
    # Retém o resultado do cursor na variável table_check
    table_check = cursor.fetchone()[0]  # fetchone() retorna uma tupla, então acessamos o primeiro elemento.
    
    cursor.close()
    cnxn.close()  # É importante fechar a conexão quando não for mais necessária.
    
    return table_check > 0




# def writes_into_sqlserver(dataframe:pd.DataFrame,tabela,cursor:object,cnxn:object,columns_names:str,question_marks:str,columns:str):
#      sleep(5)
     
#      if checa_se_tabela_existe(tabela) == 1 and checa_se_tabela_contem_dados(tabela) == False:
#           for index, row in dataframe.iterrows():              
               
#                exec(f'cursor.execute(f"INSERT INTO dbo.{tabela} {columns_names} values{question_marks}", {columns})')
#                cnxn.commit()
#           cursor.close()