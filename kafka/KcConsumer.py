import json,os
from confluent_kafka import Consumer, KafkaError


class KafkaConsumer:

    def __init__(self, kafka_brokers = "", kafka_apikey = "", topic_name = "",autocommit = True):
        self.kafka_brokers = kafka_brokers
        self.kafka_apikey = kafka_apikey
        self.topic_name = topic_name
        self.kafka_auto_commit = autocommit

    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    # Prepares de Consumer with specific options based on the case
    def prepareConsumer(self, groupID = "ConsumePlainMessagePython"):
        options ={
                'bootstrap.servers':  self.kafka_brokers,
                'group.id': groupID,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': self.kafka_auto_commit,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': 'token',
                'sasl.password': self.kafka_apikey
        }
        # Print the configuration
        print('[KafkaConsumer] - Configuration: {}'.format(options))
        # Create the consumer
        self.consumer = Consumer(options)
        # Subscribe to the topic
        self.consumer.subscribe([self.topic_name])
    
    # Prints out and returns the decoded events received by the consumer
    def traceResponse(self, msg):
        msgStr = msg.value().decode('utf-8')
        print('[KafkaConsumer] - pollNextOrder() - {} partition: [{}] at offset {} with key {}:\n\tmessage: {}'
                    .format(msg.topic(), msg.partition(), msg.offset(), str(msg.key()), msgStr ))
        return msgStr

    # Polls for next event
    def pollNextEvent(self):
        # Poll for messages
        msg = self.consumer.poll(timeout=10.0)
        # Validate the returned message
        if msg is None:
            print("[KafkaConsumer] - No new messages on the topic")
        elif msg.error():
            if ("PARTITION_EOF" in msg.error()):
                print("[KafkaConsumer] - End of partition")
            else:
                print("[KafkaConsumer] - Consumer error: {}".format(msg.error()))
        else:
            # Print the message
            msgStr = self.traceResponse(msg)

    # Polls for events until it finds an event with same key
    def pollNextEventByKey(self, keyID):
        if (str(keyID) == ""):
            print("[KafkaConsumer] - Consumer error: Key is an empty string")
            return None
        gotIt = False
        anEvent = {}
        while not gotIt:
            msg = self.consumer.poll(timeout=10.0)
            # Continue if we have not received a message yet
            if msg is None:
                continue
            if msg.error():
                print("[KafkaConsumer] - Consumer error: {}".format(msg.error()))
                # Stop reading if we find end of partition in the error message
                if ("PARTITION_EOF" in msg.error()):
                    gotIt= True
                continue
            msgStr = self.traceResponse(msg)
            # Create the json event based on message string formed by traceResponse
            anEvent = json.loads(msgStr)
            # If we've found our event based on keyname and keyID, stop reading messages
            if (str(msg.key().decode('utf-8')) == keyID):
                gotIt = True
        return anEvent

    # Polls for events endlessly
    def pollEvents(self):
        gotIt = False
        while not gotIt:
            msg = self.consumer.poll(timeout=10.0)
            if msg is None:
                continue
            if msg.error():
                print("[ERROR] - [KafkaConsumer] - Consumer error: {}".format(msg.error()))
                if ("PARTITION_EOF" in msg.error()):
                    gotIt= True
                continue
            self.traceResponse(msg)
    
    def close(self):
        self.consumer.close()