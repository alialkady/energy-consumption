from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Read data from Kafka topic
kafka_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "ed-kafka:29092") \
    .option("subscribe", "smart-home") \
    .load()

# Deserialize the value column from Kafka as a string
messages_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as message")

# Print the messages to the console
query = messages_df.writeStream.outputMode("append").format("console").start()

# Wait for the query to terminate
query.awaitTermination()
