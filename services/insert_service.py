from elasticsearch import Elasticsearch
import boto3
import pandas as pd

es = Elasticsearch('http://localhost:9200')

s3 = boto3.client('s3')

def insert_data():
    response = s3.get_object(Bucket='appventas-searcher-s3', Key='products.json')
    df = pd.read_json(response['Body'], orient='records', dtype=str)
    productos = df.to_dict(orient='records')

    for producto in productos:
        if producto.get("color") != None:
            color = producto.get("color").lower() if producto.get("color") != "None" else None
            if color != None:
                colores = color.split('/')
            
        es.index(index='productos', id=producto.get("id_code"), body={
            "sku": producto.get("sku"),
            "nombre": producto.get("nombre"),
            "tag": producto.get("tag") if producto.get("tag") != "None" else None,
            "num_puestos": int(producto.get("num_puestos")) if producto.get("num_puestos") != "None" else None,
            "color_mueble": producto.get("color_mueble").lower() if producto.get("color_mueble") != "None" else None,
            "color_tela": producto.get("color_tela").lower() if producto.get("color_tela") != "None" else None,
            "color": colores if len(colores) != 0 else None,
            "tipo_tela": producto.get("tipo_tela").lower() if producto.get("tipo_tela") != "None" else None,
            "estilo_vida": producto.get("estilo_vida").lower() if producto.get("estilo_vida") != "None" else None,
            "medida": producto.get("medida").lower() if producto.get("medida") != "None" else None,
            "firmeza": producto.get("firmeza").lower() if producto.get("firmeza") != "None" else None,
            "unidad_interna": producto.get("unidad_interna").lower() if producto.get("unidad_interna") != "None" else None,
            "marca_colchon": producto.get("marca_colchon").lower() if producto.get("marca_colchon") != "None" else None,
            "categoria": producto.get("categoria").lower() if producto.get("categoria") != "None" else None,
            "precio_grupo": float(producto.get("precio_grupo")) if producto.get("precio_grupo") != "None" else None,
            "precio_base": float(producto.get("precio_base")) if producto.get("precio_base") != "None" else None,
            "image": producto.get("image").replace('"', '') if producto.get("image") != "None" else None,
            "cod_pais": producto.get("pais"),
            "cod_tienda": producto.get("tienda")
        })
