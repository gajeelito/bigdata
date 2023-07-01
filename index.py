
from pymongo.server_api import ServerApi
from flask import Flask, request, render_template
from flask_cors import CORS
import socketio
from pymongo import MongoClient
import json
import eventlet

app = Flask(__name__, template_folder='template')
CORS(app)
sio = socketio.Server(cors_allowed_origins='*')

@sio.on('connect')
def connect(sid, environ):
    print('A user connected')

@sio.on('disconnect')
def disconnect(sid):
    print('A user disconnected')

@sio.on('consumer_data_event')
def consumer_data_event(sid, data):
    print('Received data from consumer:', data)
    uri = "mongodb+srv://pablvalenzuela:lascumbres2312@cluster0.cceutla.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri)
    try:
        db = client.am
        users = db.users
        data = users.insert_one(data)
        print('Data inserted into MongoDB:', data)
    except Exception as e:
        print('Error inserting data into MongoDB:', e)


@app.route('/')
def index():
    return render_template('templates.html')


@app.route('/historical_data')
def get_historical_data():
    # Obtener los datos históricos desde la base de datos MongoDB
    uri = "mongodb+srv://pablvalenzuela:lascumbres2312@cluster0.cceutla.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri)
    
    db = client.am
    users = db.users
    data = list(users.find())
    # Enviar los datos históricos al cliente a través del servidor Socket.IO
    socketio.emit('historical_data', data)
    
    return 'Datos históricos enviados'



if __name__ == '__main__':
    socketio_app = socketio.WSGIApp(sio, app)
    app = socketio.Middleware(sio, app)
    eventlet.wsgi.server(eventlet.listen(('localhost', 3000)), socketio_app)
