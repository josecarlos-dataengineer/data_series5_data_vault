from deltalake import DeltaTable
from flask import request, jsonify, Flask
import json

app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/read-delta-table', methods=['GET'])
def home():
    dt = DeltaTable(r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_5_data_vault\etl\4_gold\dev\obt")

    pd = dt.to_pyarrow_dataset().to_table().to_pandas()

    json_str = pd.to_json(orient = "records")

    parsed = json.loads(json_str)  

    return jsonify(parsed)

app.run()