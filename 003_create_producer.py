from kafka import KafkaProducer

productor = KafkaProducer(bootstrap_servers='localhost:9092')

lista_frutas = ['manzana','pera','uva']

# recorrer la lista
for fruta in lista_frutas:
    productor.send('topico_frutas', bytes(fruta, 'utf-8'))

productor.flush() # esperar que se complete todas las solicitudes

productor.close() # cerrar conexion