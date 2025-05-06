from elasticsearch import Elasticsearch
import boto3
import pandas as pd
import os
import ast

es = Elasticsearch('http://localhost:9200')

s3 = boto3.client('s3')

def insert_data():
    response = s3.get_object(Bucket='appventas-searcher-s3', Key='products.json')
    df = pd.read_json(response['Body'], orient='records', dtype=str)
    productos = df.to_dict(orient='records')

    for producto in productos:
        string = producto.get("precios_base")
        string2 = producto.get("valor_cuota")
        array = ast.literal_eval(string)
        array_cuotas = ast.literal_eval(string2)
        tiendas = []
        pais = []

        for e in array:
            tiendas.append(e.get("agency"))

        if "99" in tiendas:
            if len(tiendas) == 1:
                pais = ["JP"]
            else:
                pais = ["JA", "JP"]
        else:
            pais = ["JA"]
            
        es.index(index='productos', id=producto.get("id_code"), body={
            "sku": producto.get("sku"),
            "nombre": producto.get("nombre"),
            "abrv_nombre": producto.get("nombre").lower(),
            "nombre_keyword": producto.get("nombre").lower(),
            "tags": producto.get("tags") if producto.get("tags") != "None" else None,
            "num_puestos": int(producto.get("num_puestos")) if producto.get("num_puestos") != "None" else None,
            "color_mueble": producto.get("color_mueble").lower() if producto.get("color_mueble") != "None" else None,
            "color_tela": producto.get("color_tela").lower() if producto.get("color_tela") != "None" else None,
            "tipo_tela": producto.get("tipo_tela").lower() if producto.get("tipo_tela") != "None" else None,
            "estilo_vida": producto.get("estilo_vida").lower() if producto.get("estilo_vida") != "None" else None,
            "medida": producto.get("medida").lower() if producto.get("medida") != "None" else None,
            "firmeza": producto.get("firmeza").lower() if producto.get("firmeza") != "None" else None,
            "unidad_interna": producto.get("unidad_interna").lower() if producto.get("unidad_interna") != "None" else None,
            "marca_colchon": producto.get("marca_colchon").lower() if producto.get("marca_colchon") != "None" else None,
            "linea": producto.get("linea").lower() if producto.get("linea") != "None" else None,
            "sublinea": producto.get("sublinea").lower() if producto.get("sublinea") != "None" else None,
            "carrousel": producto.get("imagenes_extra").lower() if producto.get("imagenes_extra") != "None" else None,
            "precio_lista_39": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "39"), None),
            "precio_oferta_39": next((precio.get("precio") for precio in array if precio.get("agency") == "39"), None),
            "cuota_39": next((cuota.get("cuota") for cuota in array_cuota if cuota.get("agency") == "39"), None),
            "precio_lista_01": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "01"), None),
            "precio_oferta_01": next((precio.get("precio") for precio in array if precio.get("agency") == "01"), None),
            "precio_lista_A9": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "A9"), None),
            "precio_oferta_A9": next((precio.get("precio") for precio in array if precio.get("agency") == "A9"), None),
            "precio_lista_85": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "85"), None),
            "precio_oferta_85": next((precio.get("precio") for precio in array if precio.get("agency") == "85"), None),
            "precio_lista_F6": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "F6"), None),
            "precio_oferta_F6": next((precio.get("precio") for precio in array if precio.get("agency") == "F6"), None),
            "precio_lista_18": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "18"), None),
            "precio_oferta_18": next((precio.get("precio") for precio in array if precio.get("agency") == "18"), None),
            "precio_lista_55": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "55"), None),
            "precio_oferta_55": next((precio.get("precio") for precio in array if precio.get("agency") == "55"), None),
            "precio_lista_65": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "65"), None),
            "precio_oferta_65": next((precio.get("precio") for precio in array if precio.get("agency") == "65"), None),
            "precio_lista_67": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "67"), None),
            "precio_oferta_67": next((precio.get("precio") for precio in array if precio.get("agency") == "67"), None),
            "precio_lista_A3": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "A3"), None),
            "precio_oferta_A3": next((precio.get("precio") for precio in array if precio.get("agency") == "A3"), None),
            "precio_lista_99": next((precio.get("prc_lista") for precio in array if precio.get("agency") == "99"), None),
            "precio_oferta_99": next((precio.get("precio") for precio in array if precio.get("agency") == "99"), None),
            "image": producto.get("image").replace('"', '') if producto.get("image") != "None" else None,
            "cod_pais": pais,
            "cod_tienda": tiendas
        })

def download_synonyms():
    synonyms_path = "home/ubuntu/elasticsearch-8.xx.x/config/synonyms/synonyms.txt"
    bucket_name = "appventas-searcher-s3"
    file_key = "synonyms.txt"

    if os.path.exists(synonyms_path):
        os.remove(synonyms_path)
        s3.download_file(bucket_name, file_key, synonyms_path)
    else:
        s3.download_file(bucket_name, file_key, synonyms_path)
