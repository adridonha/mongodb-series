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

# Calcular la nota media de todas las series
series = list(coleccion.find({}))
nota_total = 0
total_series = 0

for serie in series:
    nota = serie.get("puntuacion", 0)
    nota_total += nota
    total_series += 1
nota_media = nota_total / total_series if total_series > 0 else 0
print(f"La nota media de todas las series es: {nota_media}")

# Crear colección detalles_produccion
detalles_coleccion = baseDatos["detalles_produccion"]

with open('detalles.json', 'r') as f:
    detalles = json.load(f)

inserted_detalles = 0
for detalle in detalles:
    if detalles_coleccion.find_one({"titulo": detalle["titulo"]}) is None:
        detalles_coleccion.insert_one(detalle)
        inserted_detalles += 1

print(f"Se insertaron {inserted_detalles} detalles de producción nuevos.")

# Consulta unificada: Series finalizadas, puntuación > 8, de EE.UU.
pipeline = [
    {
        "$lookup": {
            "from": "detalles_produccion",
            "localField": "titulo",
            "foreignField": "titulo",
            "as": "detalles"
        }
    },
    {
        "$match": {
            "finalizada": True,
            "puntuacion": {"$gt": 8},
            "detalles.pais_origen": "EE.UU."
        }
    }
]

consulta_unificada = list(coleccion.aggregate(pipeline))
for c in consulta_unificada:
    c['_id'] = str(c['_id'])
    for detalle in c.get('detalles', []):
        detalle['_id'] = str(detalle['_id'])
with open('consulta_unificada.json', 'w') as f:
    json.dump(consulta_unificada, f)

# Calcular gasto financiero de las series
pipeline_gasto = [
    {
        "$lookup": {
            "from": "detalles_produccion",
            "localField": "titulo",
            "foreignField": "titulo",
            "as": "detalles"
        }
    },
    {
        "$match": {
            "detalles": {"$ne": []}  # Solo series con detalles
        }
    },
    {
        "$addFields": {
            "total_episodios": {"$multiply": ["$temporadas", 10]},  # Asumiendo 10 episodios por temporada
            "presupuesto_episodio": {"$arrayElemAt": ["$detalles.presupuesto_por_episodio", 0]}
        }
    },
    {
        "$addFields": {
            "coste_total": {"$multiply": ["$presupuesto_episodio", "$total_episodios"]}
        }
    },
    {
        "$project": {
            "_id": 0,
            "titulo": 1,
            "coste_total": 1
        }
    }
]

gasto_series = list(coleccion.aggregate(pipeline_gasto))
with open('gasto_financiero.json', 'w') as f:
    json.dump(gasto_series, f, indent=4)

print(f"Se calcularon los costes de {len(gasto_series)} series y se guardaron en gasto_financiero.json")

