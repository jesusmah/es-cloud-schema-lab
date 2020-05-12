import os, time, sys
from kafka.KcProducer import KafkaProducer

print(" @@@ Executing script: producePlainMessage.py")

####################### READ ENV VARIABLES #######################
# Try to read the Kafka broker from the environment variables
try:
    KAFKA_BROKERS = os.environ['KAFKA_BROKERS']
except KeyError:
    print("[ERROR] - The KAFKA_BROKERS environment variable needs to be set.")
    exit(1)

# Try to read the Kafka API key from the environment variables
try:
    KAFKA_APIKEY = os.environ['KAFKA_APIKEY']
except KeyError:
    print("[ERROR] - The KAFKA_APIKEY environment variable needs to be set")
    exit(1)


####################### VARIABLES #######################
TOPIC_NAME="test"

####################### FUNCTIONS #######################
# Create a default container
def createEvent():
    
    print('Creating event...')
    
    event = {
            "eventKey" : "1", 
            "message" : "This is a test message"
            }
    
    print("DONE")
    
    return event

# Parse arguments to get the Kafka topic
def parseArguments():
    global TOPIC_NAME
    print("The arguments for this script are: " , str(sys.argv))
    if len(sys.argv) == 2:
        TOPIC_NAME = sys.argv[1]
    else:
        print("[ERROR] - The producePlainMessage.py script expects one argument: The Kafka topic to publish the message to")
        exit(1)

####################### MAIN #######################
if __name__ == '__main__':
    # Get the Kafka topic from the arguments
    parseArguments()
    # Create the event to be sent
    event = createEvent()
    # Print it out
    print("Event to be published:")
    print(event)
    # Create the Kafka Producer
    kp = KafkaProducer(KAFKA_BROKERS,KAFKA_APIKEY)
    # Prepare the Kafka Producer
    kp.prepareProducer("ProducePlainMessagePython")
    # Publish the event
    kp.publishEvent(TOPIC_NAME,event,"eventKey")
