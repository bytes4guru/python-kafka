from confluent_kafka import Consumer, KafkaError

# Configure the Kafka consumer
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'my_consumer_group',        # Consumer group ID
    'auto.offset.reset': 'earliest'         # Start reading from the earliest message
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the topic
consumer.subscribe(['my_topic'])

# Poll messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages, wait for 1 second
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached {msg.topic()} [{msg.partition()}]")
            else:
                print(f"Error: {msg.error()}")
        else:
            print(f"Received message: {msg.value().decode('utf-8')} from {msg.topic()} [{msg.partition()}]")

except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets and leave the group cleanly
    consumer.close()
