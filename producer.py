# import tweepy
from json import dumps
import logging
from datetime import datetime
import json
from dotenv import dotenv_values
import tweepy
import kafka

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ::%(levelname)s::%(name)s --> %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
server = '35.175.217.207'

config = dotenv_values(".env")
twitter_bearer_token = config["TWITTER_BEARER_TOKEN"]
topics_name = ['Politics', 'Health', 'School']

print(topics_name[0])

def connect_to_kafka(server):
    try:
        producer = kafka.KafkaProducer(
            bootstrap_servers=server,
            # value_serializer = lambda x:dumps(x).encode('utf-8')
        )
        return producer
    
    except kafka.errors.NoBrokersAvailable:
        raise kafka.errors.NoBrokersAvailable("No brokers available at the specified url: {server}")


def send_message(producer: kafka.KafkaProducer, topic: str, message:bytes):
    producer.send(topic, message)


class TwitterStreamingClient(tweepy.StreamingClient):

    def __init__(self, bearer_token, kafka_producer, kafka_topic, *args, **kwargs):
        super().__init__(bearer_token, *args, **kwargs)
        self.producer = kafka_producer
        self.topic = kafka_topic

        print(self.topic, self.producer)

    def on_data(self, raw_data):
        # send_message(raw_data)
        print(type(raw_data), raw_data)
        
        data = json.loads(raw_data)
        print(data)
        send_message(self.producer, self.topic, raw_data)

    def on_connection_error(self):
        print(self.consumer_key)
        logger.info("A connection error occured")

    def on_request_error(self, status_code):
        print(self.consumer_key)
        logger.info("An error of status {} occured".format(status_code))

try:
    producer = connect_to_kafka(server)
    stream_politics = TwitterStreamingClient(twitter_bearer_token , producer, topics_name[0])
    stream_politics.add_rules(tweepy.StreamRule(topics_name[0]))
    stream_politics.filter()

    stream_health = TwitterStreamingClient(twitter_bearer_token, producer, topics_name[1])
    stream_health.add_rules(tweepy.StreamRule(topics_name[1]))
    stream_health.filter()

    stream_school = TwitterStreamingClient(twitter_bearer_token, producer, topics_name[2])
    stream_school.add_rules(tweepy.StreamRule(topics_name[2]))
    stream_school.filter()
except KeyboardInterrupt:
    stream_politics.delete_rules(topics_name[0])
    stream_politics.delete_rules(topics_name[1])
    stream_politics.delete_rules(topics_name[2])
except Exception as e:
        print(e)
