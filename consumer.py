from kafka import KafkaConsumer
from json import loads
import logging
from dotenv import dotenv_values
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ::%(levelname)s::%(name)s --> %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
try:
    consumer1 = KafkaConsumer(
        'Politics', bootstrap_servers='35.175.217.207', group_id='my-group', value_deserializer = lambda x : loads(x.decode('utf-8'))
        )
    consumer1.subscribe(topics="Politics")
    for message in consumer1:
        logger.info(message.value)
except Exception as e:
    logger.warning(e)


