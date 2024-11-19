from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
import json

# Configuration Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'heart-rate-consumer',
    'auto.offset.reset': 'earliest'
}

# Configuration MongoDB
mongo_client = MongoClient('mongodb://admin:admin@localhost:27017')  # Adresse de MongoDB
db = mongo_client['heart_rate_db']  # Nom de la base de données
collection = db['heart_rate_collection']  # Nom de la collection

# Créer le consommateur
consumer = Consumer(conf)

# S'abonner à un ou plusieurs topics
consumer.subscribe(['heart-rate-data'])

# Fonction pour consommer les messages et les insérer dans MongoDB
def consume_messages():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition} at offset {msg.offset}")
                else:
                    raise KafkaException(msg.error())
            else:
                message_value = msg.value().decode('utf-8')  # Décoder le message
                print(f"Received message: {message_value}")

                try:
                    # Convertir le message en JSON
                    message_json = json.loads(message_value)

                    # Insérer dans MongoDB
                    collection.insert_one(message_json)
                    print("Message inserted into MongoDB.")
                except json.JSONDecodeError:
                    print("Failed to decode message as JSON.")
                except Exception as e:
                    print(f"Failed to insert message into MongoDB: {e}")

    except KeyboardInterrupt:
        print("Consuming stopped.")
    finally:
        consumer.close()
        mongo_client.close()

# Lancer la consommation des messages
consume_messages()