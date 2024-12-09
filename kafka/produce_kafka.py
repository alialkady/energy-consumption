from kafka import KafkaProducer
import json
from energy_consumption import data_filtered_10
# Kafka configuration
kafka_broker = 'ed-kafka:29092'
topic_name = 'smart-home'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda value: value.encode('utf-8')
)

# Function to send data from the DataFrame to Kafka
def send_messages_from_dataframe(df, topic):
    if df.empty:
        print("The DataFrame is empty. No messages to send.")
        return
    
    for index, row in df.iterrows():
        try:
            # Convert each row to a dictionary and then to a JSON string
            message = json.dumps(row.to_dict())
            producer.send(topic, value=message)
            print(f"Sent: {message}")
        except Exception as e:
            print(f"Error sending message at index {index}: {e}")

# Send data from data_filtered_10 DataFrame
print("Starting to send messages...")
send_messages_from_dataframe(data_filtered_10, topic_name)

# Close the producer
producer.close()
print("Finished producing messages.")
