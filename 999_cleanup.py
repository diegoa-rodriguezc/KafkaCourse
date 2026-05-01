"""
999 - Limpiar todos los topics creados durante el curso.

Aprendizaje:
    Como borrar varios topics en una sola llamada. Util para volver
    a empezar desde cero sin tener que reiniciar el contenedor.

Antes de ejecutar:
    - Tener en ejecución el Docker de kafka (docker compose -f docker-compose1.yml up -d)

Resultado:
    Borra 'python_topic', 'topico_particiones' y 'topico_frutas' si existen.
    No toca los topics internos de Kafka (los que empiezan con '__').
"""

# importar librerias
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError, NoBrokersAvailable

# Definir variables globales
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS_DEL_CURSO = ['python_topic', 'topico_particiones', 'topico_frutas']

print(f'Conectando al broker en {BOOTSTRAP_SERVERS}...')
try:
    cliente_kafka = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
except NoBrokersAvailable:
    print('ERROR: no se pudo contactar al broker. Esta arriba el contenedor de Kafka?')
    raise SystemExit(1)

existentes = set(cliente_kafka.list_topics())
a_borrar = [t for t in TOPICS_DEL_CURSO if t in existentes]

if not a_borrar:
    print('No hay topics del curso para borrar.')
else:
    print(f'Borrando: {", ".join(a_borrar)}')
    try:
        cliente_kafka.delete_topics(topics=a_borrar)
        print('OK: topics eliminados.')
    except UnknownTopicOrPartitionError as e:
        print(f'AVISO: alguno no existia. Detalle: {e}')

cliente_kafka.close()
