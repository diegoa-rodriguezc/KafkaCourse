"""
002 - Crear un topic con varias particiones.

Aprendizaje:
    Las particiones permiten paralelismo: cada particion puede ser leida
    por un consumidor distinto del mismo grupo. Aqui creamos un topic
    con 5 particiones; abrir en Kafdrop (http://localhost:9000) para verlo.

Antes de ejecutar:
    - Tener en ejecución el Docker de kafka (docker compose -f docker-compose1.yml up -d)
"""

# importar librerias
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Definir variables globales
BOOTSTRAP_SERVERS = 'localhost:9092'
NOMBRE_TOPIC = 'topico_particiones'
NUM_PARTICIONES = 5

print(f'Conectando al broker en {BOOTSTRAP_SERVERS}...')
try:
    cliente_kafka = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id='kafka-python',
    )
except NoBrokersAvailable:
    print('ERROR: no se pudo contactar al broker. Esta arriba el contenedor de Kafka?')
    raise SystemExit(1)

# crear topic con particiones definidas
topic = NewTopic(name=NOMBRE_TOPIC, num_partitions=NUM_PARTICIONES, replication_factor=1)

try:
    cliente_kafka.create_topics(new_topics=[topic])
    print(f'OK: topic "{NOMBRE_TOPIC}" creado con {NUM_PARTICIONES} particiones.')
    print('Abrir Kafdrop en http://localhost:9000 para verlo.')
except TopicAlreadyExistsError:
    print(f'AVISO: el topic "{NOMBRE_TOPIC}" ya existia, no se hizo nada.')
finally:
    cliente_kafka.close()
