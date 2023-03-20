from pyspark.sql.types import StructType, StringType
from pyspark.sql import *
from pyspark.sql.functions import col, from_json, explode, substring
import findspark
from pyspark.sql.types import *
findspark.init()
import logging

from dotenv import dotenv_values
config = dotenv_values(".env")


access_key = config["ACCESS_KEY"]
secret_key = config["SECRET_KEY"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ::%(levelname)s::%(name)s --> %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# create spark stream app
spark = (
    SparkSession
    .builder
    .master('local')
    .appName("kafka-3-topics")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.hadoop.fs.s3a.access.key", access_key) 
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .config("spark.hadoop.fs.endpoint", "s3-us-east-1.amazonaws.com") 
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .config("spark.sql.adaptive.enabled","true")
    .getOrCreate()
)
# spark.sparkContext.setLogLevel("INFO")
sc = spark.sparkContext

# read stream 
df = (spark 
    .readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "35.175.217.207:9092")
    .option("subscribe", "Politics,School,Health") 
    .option("group.id", "my-group")
    .option("rowsPerSecond", 3)
    .option("startingOffsets", "latest") 
    
    .load()
)
logger.info("Raw data schema")
df.printSchema()

df1 = df.selectExpr(
    "CAST (value AS STRING) as new_value",
    "partition",
    "timestamp",
    "topic"
)
logger.info("First layer schema")
df1.printSchema()
schema_1 = (
        StructType()
        .add("data", MapType(StringType(), StringType()))
        .add("matching_rules", ArrayType(MapType(StringType(), StringType())))
)
df2 = (
    df1.select("*")\
    .withColumn( "new_data",  from_json(col("new_value"), schema_1))
    .withColumn("data_id", col("new_data.data").getItem("id") ) 
    .withColumn("data_text", col("new_data.data").getItem("text") ) 
    .withColumn("data_history", substring(col("new_data.data").getItem("edit_history_tweet_ids"), 3, 20) )
    .withColumn("matching_rules", explode("new_data.matching_rules")) 
    .withColumn("matching_rules_id", col("matching_rules").getItem("id") )
    .withColumn("matching_rules_tag", col("matching_rules").getItem("tag") )  
)
logger.info("Second layer schema")
df2.printSchema()

final_df = df2.select(
    "topic",
    "partition",
    "data_text",
    "data_id",
    "data_history",
    "matching_rules_id",
    "matching_rules_tag",
    "timestamp"
)
logger.info("Final schema")
final_df.printSchema()

bucket_names = [
    "s3a://adis-demo-school/",
    "s3a://adis-demo-politics/",
    "s3a://adis-demo-health/"
]
def different_s3(df, epoch_id):
    df.filter(col("topic")=="School").write.mode("append").format("parquet").option("path", bucket_names[0]).save()
    df.filter(col("topic")=="Politics").write.mode("append").format("parquet").option("path", bucket_names[1]).save()
    df.filter(col("topic")=="Health").write.mode("append").format("parquet").option("path", bucket_names[2]).save()
    return ""

query = final_df \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(different_s3) \
    .start()

query.awaitTermination()

# query = final_df \
#     .writeStream \
#     .trigger(processingTime='5 seconds') \
#     .outputMode("append")\
#     .option("truncate", "false") \
#     .format("console") \
#     .start()

# query.awaitTermination()
