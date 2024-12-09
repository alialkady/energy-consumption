from kafka import KafkaProducer
import pandas as pd
from energy_consumption import data_filtered
#kafka configuration
kafka_broker = 'ed-kafka:9092'
topic_name = 'smart-home'

#initialize producer
producer = KafkaProducer(bootstrap_servers=kafka_broker)

#kafka produce

for index, row in data_filtered.iterrows():
    message = row.to_json()
    producer.send(topic_name, message.encode('utf-8'))
    print(f"Sent: {message}")
    

producer.close()
print("Finished.")
