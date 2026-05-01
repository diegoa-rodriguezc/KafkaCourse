"""
003 - Producir mensajes en un topic.

Aprendizaje:
    Como crear un KafkaProducer y enviar mensajes (en bytes). Si el topic
    no existe, Kafka lo crea automaticamente con la configuracion por defecto.

Antes de ejecutar:
    - Tener en ejecución el Docker de kafka (docker compose -f docker-compose1.yml up -d)

Despues:
    - Ejecutar 004_consumer.py para leer estos mensajes.
"""

# importar librerias
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Definir variables globales
BOOTSTRAP_SERVERS = 'localhost:9092'
NOMBRE_TOPIC = 'topico_frutas'

print(f'Conectando al broker en {BOOTSTRAP_SERVERS}...')
try:
    productor = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
except NoBrokersAvailable:
    print('ERROR: no se pudo contactar al broker. Esta arriba el contenedor de Kafka?')
    raise SystemExit(1)

lista_frutas = ['manzana', 'pera', 'uva']

# recorrer la lista
for fruta in lista_frutas:
    productor.send(NOMBRE_TOPIC, bytes(fruta, 'utf-8'))
    print(f'  enviado -> {fruta}')

productor.flush() # esperar a que se completen todas las solicitudes
productor.close() # cerrar conexión
print(f'OK: {len(lista_frutas)} mensajes enviados al topic "{NOMBRE_TOPIC}".')