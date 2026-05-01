"""
005 - Listar todos los topics del cluster.

Aprendizaje:
    Como obtener la lista de topics existentes. Veras tambien topics
    internos como '__consumer_offsets' que Kafka crea por su cuenta.

Antes de ejecutar:
    - Tener en ejecución el Docker de kafka (docker compose -f docker-compose1.yml up -d)
"""

# importar librerias
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

# Definir variables globales
BOOTSTRAP_SERVERS = 'localhost:9092'

print(f'Conectando al broker en {BOOTSTRAP_SERVERS}...')
try:
    cliente_kafka = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
except NoBrokersAvailable:
    print('ERROR: no se pudo contactar al broker. Esta arriba el contenedor de Kafka?')
    raise SystemExit(1)

lista_topicos = cliente_kafka.list_topics()

print(f'Topics encontrados ({len(lista_topicos)}):')
for topico in sorted(lista_topicos):
    print(f'  - {topico}')

cliente_kafka.close() # cerrar conexión
