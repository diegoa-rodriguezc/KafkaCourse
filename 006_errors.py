"""
006 - Manejo de errores y reintentos en el productor.

Aprendizaje:
    'retries' y 'retry_backoff_ms' permiten reintentar el envio si hay
    fallos transitorios (por ejemplo, el broker tarda en responder).
    Envolvemos todo en try/except para capturar errores de conexion
    (broker caido) y mostrarlos de forma legible en lugar del stacktrace.

Antes de ejecutar:
    - Tener en ejecución el Docker de kafka (docker compose -f docker-compose1.yml up -d)
"""

# importar librerias
from kafka import KafkaProducer

# Definir variables globales
BOOTSTRAP_SERVERS = 'localhost:9092'
NOMBRE_TOPIC = 'topico_frutas'

try:
    print(f'Conectando al broker en {BOOTSTRAP_SERVERS} (con reintentos)...')
    productor = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        retries=5,
        retry_backoff_ms=100,
    )

    lista_frutas = ['manzana', 'pera', 'uva']

    # recorrer losta
    for fruta in lista_frutas:
        productor.send(NOMBRE_TOPIC, bytes(fruta, 'utf-8'))
        print(f'  enviado -> {fruta}')

    productor.flush()
    productor.close()
    print(f'OK: {len(lista_frutas)} mensajes enviados al topic "{NOMBRE_TOPIC}".')

except Exception as error:
    print(f'Error con Apache Kafka: {error}')
