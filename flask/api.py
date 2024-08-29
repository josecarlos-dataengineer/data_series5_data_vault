from flask import Flask, jsonify
from sqlalchemy import create_engine, MetaData, Table
import pyodbc
from dotenv import load_dotenv
import os

app = Flask(__name__)

# Carrega as variáveis do arquivo .env
load_dotenv(r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_5_data_vault\gerador_de_dados\.env")

# Configurações do banco de dados
server = os.getenv("server")
database = os.getenv("database")
username = os.getenv("username_")
password = os.getenv("password")
driver = os.getenv("driver")

connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
print(connection_string)
# table_name = "report1_daily_overview"
# Criação do engine do SQLAlchemy
engine = create_engine(connection_string)

# Função para obter dados de uma tabela específica
def get_table_data(table_name):
    metadata = MetaData(bind=engine)
    table = Table(table_name, metadata, autoload_with=engine)
    conn = engine.connect()
    result = conn.execute(table.select()).fetchall()
    conn.close()
    return [dict(row) for row in result]

# Endpoint para obter dados da tabela
@app.route('/api/tabela/<table_name>', methods=['GET'])
def get_data(table_name):
    try:
        data = get_table_data(table_name)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint raiz
@app.route('/')
def home():
    return "Bem-vindo à API de Dados!"

# Endpoint para favicon (opcional)
@app.route('/favicon.ico')
def favicon():
    return '', 204

if __name__ == '__main__':
    app.run(debug=True)