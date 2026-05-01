"""
000 - Crear un topic en Kafka.

Aprendizaje:
    Como conectarte al broker con KafkaAdminClient y crear un topic
    indicando numero de particiones y factor de replicacion.

Antes de ejecutar:
    Levanta el broker con:
        docker compose -f docker-compose1.yml up -d
"""

# importar librerias
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Definir variables globales
BOOTSTRAP_SERVERS = 'localhost:9092'
NOMBRE_TOPIC = 'python_topic'


print(f'Conectando al broker en {BOOTSTRAP_SERVERS}...')
# Captura de errores/excepciones en caso de falla de la conexión
try:
    cliente_kafka = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id='kafka-python',
    )
except NoBrokersAvailable:
    print('ERROR: no se pudo contactar al broker. Esta arriba el contenedor de Kafka?')
    raise SystemExit(1)

# crear topic
topic = NewTopic(name=NOMBRE_TOPIC, num_partitions=1, replication_factor=1)

# Bloque para manejo de excepciones en caso de no crear el topic
try:
    cliente_kafka.create_topics(new_topics=[topic])
    print(f'OK: topic "{NOMBRE_TOPIC}" creado (1 particion, replication factor 1).')
except TopicAlreadyExistsError:
    print(f'AVISO: el topic "{NOMBRE_TOPIC}" ya existia. Borralo con 001_dropTopic.py si quieres recrearlo.')
finally:
    cliente_kafka.close()
