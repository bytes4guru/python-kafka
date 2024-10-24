from confluent_kafka import Producer
from time import sleep
# Kafka broker configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'client.id': 'python-producer'
}

# Create Producer instance
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Produce messages
for i in range(10):
    producer.produce('my_topic', key=str(i), value=f'message {i}', callback=delivery_report)
    producer.poll(1)  # Ensures the delivery_report callback is triggered
    sleep(1)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
