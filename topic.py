from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType 
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ::%(levelname)s::%(name)s --> %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)



admin_client = KafkaAdminClient(bootstrap_servers='35.175.217.207')
topics_name = ['Politics', 'Health', 'School']

partitions = 1
replicas = 1
kafka_topic_name = list()

for i in topics_name:
    try:
        if i not in admin_client.list_topics():
            logger.info(f"New topic : `{i}` is added")
            kafka_topic_name.append(
                NewTopic(
                    name=i,
                    num_partitions=partitions, 
                    replication_factor=replicas
                    )
            )
        else:
            logger.warning(f"The topic [{i}]is existing!" )
    except Exception as e:
        logger.warning(e)

try:
    if kafka_topic_name != admin_client.list_topics():
        admin_client.create_topics(new_topics=kafka_topic_name)
    else:
        logger.warning(f"The topics existing!" )
except Exception as e:
    logger.warning("Error creating topics !")
    logger.warning(e)
        
# logger.info(admin_client.delete_topics(topics=topics_name))

# logger.info(admin_client.list_topics())
logger.info(admin_client.list_consumer_groups())
# logger.info(admin_client.describe_cluster())
# logger.info(admin_client.describe_topics(["Politics"]))

# logger.info(admin_client.describe_configs(config_resources=[ConfigResource(ConfigResourceType.TOPIC, "Politics")]))
