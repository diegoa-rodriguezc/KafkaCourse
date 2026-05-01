"""
004 - Consumir mensajes de un topic.

Aprendizaje:
    Como leer mensajes con KafkaConsumer. 'auto_offset_reset=earliest'
    hace que la primera vez leamos desde el principio del topic.
    El consumidor queda escuchando indefinidamente; pulsa Ctrl+C para salir.

Antes de ejecutar:
    - Tener en ejecución el Docker de kafka (docker compose -f docker-compose1.yml up -d)
    - Ejecutar 003_create_producer.py para tener mensajes que consumir.
"""

# importar librerias
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Definir variables globales
BOOTSTRAP_SERVERS = 'localhost:9092'
NOMBRE_TOPIC = 'topico_frutas'

print(f'Conectando al broker en {BOOTSTRAP_SERVERS}...')
try:
    consumidor = KafkaConsumer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id='python_consumidor',
        auto_offset_reset='earliest',
    )
except NoBrokersAvailable:
    print('ERROR: no se pudo contactar al broker. Esta arriba el contenedor de Kafka?')
    raise SystemExit(1)

consumidor.subscribe([NOMBRE_TOPIC])
print(f'Suscrito a "{NOMBRE_TOPIC}". Esperando mensajes... (Ctrl+C para salir)')

try:
    for mensaje in consumidor:
        print(f'  recibido <- {mensaje.value.decode("utf-8")} '
              f'(particion={mensaje.partition}, offset={mensaje.offset})')
except KeyboardInterrupt:
    print('\nDeteniendo consumidor...')
finally:
    consumidor.close()
    print('Consumidor cerrado.')
