# KafkaCourse

Proyecto para aprender **Apache Kafka** usando Python (`kafka-python`).
Cada script numerado (`000_` … `006_` y `999_`) introduce un concepto. 
Cada script sirve para levantar el broker, crear topics, producir y consumir mensajes.

---

## 1. Requisitos

- **Python 3.11** o **3.12** 
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Kafdrop (UI web) viene incluido en los `docker-compose`, y para acceder al mismo ingresar mediante un navegador a la URL http://localhost:9000

## 2. Instalación

* Si usa Anaconda y tiene una versión superior a la especificada, crea un entorno aislado:

```bash
conda create -n kafkacourse python=3.12 -y
conda activate kafkacourse
pip install -r requirements.txt
```

* Si ya tiene Python 3.11 o 3.12, ejecutar:

```bash
pip install -r requirements.txt
```

## 3. Levantar el broker

Hay **dos opciones** (eligir solo *UNA*, en ambas se usa el puerto 9092):

| Archivo               | Modo                     | Cuándo usarlo                         |
|-----------------------|--------------------------|---------------------------------------|
| `docker-compose1.yml` | Zookeeper + Kafka        | Setup básico,  para aprendizaje       |
| `docker-compose2.yml` | KRaft (sin Zookeeper)    | Setup moderno, recomendado por Apache |

Ajustar el nombre del docker-compose**N**.yml según opción elegida previamente:
* Para iniciar el docker 
```bash
# arrancar
docker compose -f docker-compose1.yml up -d
```

* Para ver el estado del compose
```bash
# ver estado
docker compose -f docker-compose1.yml ps
```

* Para detener y limpiar/eliminar volúmenes de datos
```bash
# parar y limpiar volúmenes
docker compose -f docker-compose1.yml down -v
```

Una vez iniciado el Docker, abrir un navegador Web e ir a la URL de Kafdrop: http://localhost:9000

## 4. Orden de ejecución sugerido

Comandos en orden:

```bash
python 000_create_topic.py        # crea 'python_topic'
python 005_list_topics.py         # listar topics
python 002_topic_partitions.py    # crea 'topico_particiones' con 5 particiones
python 003_create_producer.py     # produce 3 mensajes en 'topico_frutas'
python 004_consumer.py            # consume los anteriores mensajes (Ctrl+C para salir)
python 006_errors.py              # productor con reintentos y try/except
python 001_dropTopic.py           # borra 'python_topic'
python 999_cleanup.py             # borra todos los topics creados
```

Cada script tiene una breve explicación de su funcionamiento, así como el requisito para ejecutar el mismo.

## 5. CLI de Kafka dentro del contenedor

Para ejecutar y/o utilizar los comandos de Kafka dentro del contenedor, ingresar al mismo mediante el comando:
```bash
docker exec -it kafka /bin/bash
```

> [!NOTE] 
> Una vez ha ingresado al contenedor de kafka, puede ejecutar los comandos descritos a continuación

1. Crear un `topic`
```bash
kafka-topics.sh --create --topic mi_topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```
Sirve para crear un topic llamado `mi_topic`<br/>
`--create`: indica que va a crear algo <br/>
`--topic mi_topic`: nombre del topic, en este caso `mi_topic` <br/>
`--partitions 1`: número de particiones (solo 1 en este caso) <br/>
`--replication-factor 1`: cuántas copias del topic existen (1 = sin redundancia) <br/>
`--bootstrap-server localhost:9092`: dirección del broker de Kafka


2. Enviar mensajes (producer)
```bash
kafka-console-producer --broker-list localhost:9092 --topic mi_topic
```
Abre una consola para enviar mensajes al topic <br/>
`--broker-list`: dirección del servidor Kafka <br/>
`--topic mi_topic`: topic al que vas a escribir, en este caso `mi_topic`

3. Leer mensajes (consumer)
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic topicdocker --from-beginning
```
Sirve para leer mensajes del topic <br/>
`--bootstrap-server`: servidor Kafka <br/>
`--topic mi_topic`: topic a leer, en este caso `mi_topic` <br/>
`--from-beginning`: lee todos los mensajes desde el inicio (no solo los nuevos) <br/>