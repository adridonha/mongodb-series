from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

load_dotenv()

usuario= os.getenv("USUARIO_MONGODB")
password = os.getenv("PASSWORD_MONGODB")
cluster= os.getenv("CLUSTER_MONGODB")
cliente = MongoClient('mongodb+srv://'+usuario+':'+password+'@'+cluster+'.lhx29hp.mongodb.net/?appName='+cluster)

try:
   cliente.admin.command('ping')
   print("NOS CONECTAMOS CORRECTAMENTE")
except Exception as e:
   print(e)

baseDatos = cliente["TV_StreamDB"]
coleccion = baseDatos["series"]
documento = { "nombre": "Adrián", "apellido": "González" }

with open('series.json', 'r') as f:
    series = json.load(f)

inserted_count = 0
for serie in series:
    if coleccion.find_one({"titulo": serie["titulo"]}) is None:
        coleccion.insert_one(serie)
        inserted_count += 1

print(f"Se insertaron {inserted_count} series nuevas.")

maratones_largas = list(coleccion.find({"temporadas": {"$gt": 5}, "puntuacion": {"$gt": 8.0}}))
for m in maratones_largas:
    m['_id'] = str(m['_id'])
with open('maratones.json', 'w') as f:
    json.dump(maratones_largas, f)

joyas_comedia = list(coleccion.find({"genero": {"$in": ["Comedy"]}, "año_estreno": {"$gte": 2020}}))
for j in joyas_comedia:
    j['_id'] = str(j['_id'])
with open('comedias_recientes.json', 'w') as f:
    json.dump(joyas_comedia, f)

contenido_finalizado = list(coleccion.find({"finalizada": True}))
for c in contenido_finalizado:
    c['_id'] = str(c['_id'])
with open('series_finalizadas.json', 'w') as f:
    json.dump(contenido_finalizado, f)

sci_fi_alta = list(coleccion.find({"genero": {"$in": ["Sci-Fi"]}, "puntuacion": {"$gte": 8.5}}))
for s in sci_fi_alta:
    s['_id'] = str(s['_id'])
with open('inventada.json', 'w') as f:
    json.dump(sci_fi_alta, f)
