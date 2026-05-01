"""
001 - Eliminar un topic en Kafka.

Aprendizaje:
    Como borrar un topic existente y manejar el caso de que no exista.

Antes de ejecutar:
    - Tener en ejecución el Docker de kafka (docker compose -f docker-compose1.yml up -d)
    - Ejecutar el 000_create_topic.py primero, para tener algo que borrar.
"""

# importar librerias
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError, NoBrokersAvailable

# Definir variables globales
BOOTSTRAP_SERVERS = 'localhost:9092'
NOMBRE_TOPIC = 'python_topic'

print(f'Conectando al broker en {BOOTSTRAP_SERVERS}...')
try:
    cliente_kafka = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
except NoBrokersAvailable:
    print('ERROR: no se pudo contactar al broker. Esta arriba el contenedor de Kafka?')
    raise SystemExit(1)

# Bloque de manejo de excepciones/errores en caso de no poder eliminar el topic
try:
    cliente_kafka.delete_topics(topics=[NOMBRE_TOPIC])
    print(f'OK: topic "{NOMBRE_TOPIC}" eliminado.')
except UnknownTopicOrPartitionError:
    print(f'AVISO: el topic "{NOMBRE_TOPIC}" no existe, no se hizo nada.')
finally:
    cliente_kafka.close()
