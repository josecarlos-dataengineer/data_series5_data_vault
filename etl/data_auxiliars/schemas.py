import pyarrow as pa

SCHEMAS = {
    "dim_clientes":
        {
        "table_id":pa.string(),
        "chave":pa.string(),
        "nome":pa.string(),
        "idade":pa.int64(),
        "genero":pa.string(),
        "uf":pa.string(),
        "cidade":pa.string(),
        "load_date":pa.timestamp('us'),
        "creation_date":pa.timestamp('us')
        },
        
    "dim_produtos":
        {
        "table_id":pa.string(),
        "produto":pa.string(),
        "tamanho":pa.string(),
        "colecao":pa.string(),
        "modelo":pa.string(),
        "categoria":pa.string(),
        "chave":pa.string(),
        "custo":pa.float64(),
        "load_date":pa.timestamp('us'),
        "creation_date":pa.timestamp('us')
        },
    "dim_vendedores":
        {
        "table_id":pa.string(),
        "chave":pa.string(),
        "nome":pa.string(),
        "idade":pa.int64(),
        "genero":pa.string(),
        "uf":pa.string(),
        "cidade":pa.string(),
        "load_date":pa.timestamp('us'),
        "creation_date":pa.timestamp('us')
        },
    "fato_vendas":
        {
        # "table_id":pa.string(),
        "id_venda":pa.string(),
        "id_produto":pa.string(),
        "id_cliente":pa.string(),
        "id_vendedor":pa.string(),
        "quantidade":pa.int64(),
        "data_venda":pa.timestamp('us'),
        "lucro":pa.float64(),
        "comissao_negociada":pa.float64(),
        "load_date":pa.timestamp('us')
        },
            }


