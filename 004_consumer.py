from kafka import KafkaConsumer

consumidor = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    group_id='python_consumidor',
    auto_offset_reset='earliest'
)

consumidor.subscribe(['topico_frutas'])

# revisar los mensajes del topic
for mensaje in consumidor:
    print(mensaje.value.decode('utf-8'))

consumidor.close()