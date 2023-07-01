import socketio
from pymongo import MongoClient
from confluent_kafka import Consumer

d1 = {
   "bootstrap.servers":"pkc-03gz2.southamerica-west1.gcp.confluent.cloud:9092",
   "security.protocol":"SASL_SSL",
   "sasl.mechanisms":"PLAIN",
   "sasl.username":"JQXUW4CNJNF5J527",
   "sasl.password":"RLLtdvhEqFDc36b0kD6LOHvq6ku5xaTZZ8wWrHzPO4lERj9eNEEnQ4Folvw7FPT8"
}

sio = socketio.Client()

props = d1
props["group.id"] = "python-group-1"
props["auto.offset.reset"] = "earliest"

consumer = Consumer(props)
consumer.subscribe(["topic_1"])

@sio.event
def connect():
    print('Conectado al servidor Socket.IO')

@sio.event
def disconnect():
    print('Desconectado del servidor Socket.IO')

@sio.event
def consumer_data_event(data):
  print('Received data from producer:', data)
  uri = "mongodb+srv://pablvalenzuela:lascumbres2312@cluster0.cceutla.mongodb.net/?retryWrites=true&w=majority"
  client = MongoClient(uri)
  try:
    db = client.am
    users = db.users
    data = users.insert_one(data)
    print('Datos insertados:', data)
  except Exception as e:
     print('Error en insertar datos:', e)


@sio.on('connect')
def connect_handler():
  print('Connected to Socket.IO server')
  while True:
    msg = consumer.poll(1.0)
    if msg is not None and msg.error() is None:
      key = msg.key().decode('utf-8')
      value = msg.value().decode('utf-8')
      print("Key: {key}, Value: {value}".format(key=key, value=value))
      sio.emit('consumer_data_event', {'key': key, 'value': value})

try:
  sio.connect('http://localhost:3000')
  sio.wait()
except KeyboardInterrupt:
   pass
finally:
  consumer.close()





