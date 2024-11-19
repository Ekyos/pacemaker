from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime, timedelta
import threading

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'heart-rate-simulator'
}

producer = Producer(conf)

alert_codes = ["A01", "B02", "C03", "D04", "E05"]

def generate_heart_rate_data(pacemaker_id):
    measure_nature = random.choice(["STANDARD", "INCIDENT", "ALERT"])
    date = datetime.utcnow() + timedelta(seconds=random.randint(-3600, 3600))
    alert_level = random.randint(1, 4)
    alert_code = random.choice(alert_codes)
    heart_rate = random.randint(60, 100)
    body_temperature = round(random.uniform(36.0, 39.5), 2)
    
    return {
        'measureNature': measure_nature,
        'pacemakerId': pacemaker_id,
        'date': date.isoformat(),
        'alertLevel': alert_level,
        'alertCode': alert_code,
        'heartRate': heart_rate,
        'bodyTemperature': body_temperature
    }

def send_to_kafka(pacemaker_id):
    while True:
        data = generate_heart_rate_data(pacemaker_id)
        print(f"Sending data for pacemaker {pacemaker_id}: {data}")
        producer.produce('heart-rate-data', value=json.dumps(data))
        producer.flush()
        time.sleep(random.randint(1, 5))  # simulate a random delay between sends

# Nombre de simulateurs à simuler
num_simulators = 5

# Lancer un thread pour chaque simulateur
threads = []
for pacemaker_id in range(1, num_simulators + 1):
    thread = threading.Thread(target=send_to_kafka, args=(pacemaker_id,))
    threads.append(thread)
    thread.start()

# Attendre que tous les threads terminent
for thread in threads:
    thread.join()