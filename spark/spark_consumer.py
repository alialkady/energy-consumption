from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum
import mysql.connector
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
# Initialize Spark session
spark = SparkSession.builder.appName("energy_consumption").getOrCreate()

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("use", DoubleType(), True),
    StructField("gen", DoubleType(), True),
    StructField("House overall", DoubleType(), True),
    StructField("Dishwasher", DoubleType(), True),
    StructField("Furnace 1", DoubleType(), True),
    StructField("Furnace 2", DoubleType(), True),
    StructField("Home office", DoubleType(), True),
    StructField("Fridge", DoubleType(), True),
    StructField("Wine cellar", DoubleType(), True),
    StructField("Garage door", DoubleType(), True),
    StructField("Kitchen 12", DoubleType(), True),
    StructField("Kitchen 14", DoubleType(), True),
    StructField("Kitchen 38", DoubleType(), True),
    StructField("Barn", DoubleType(), True),
    StructField("Well", DoubleType(), True),
    StructField("Microwave", DoubleType(), True),
    StructField("Living room", DoubleType(), True),
    StructField("Solar", DoubleType(), True)
])

# Read data from Kafka topic
kafka_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "ed-kafka:29092") \
    .option("subscribe", "smart-home") \
    .load()

# Deserialize the JSON data from the Kafka messages
messages_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

# Perform aggregation: sum all values for each column
aggregated_df = messages_df.agg(
    spark_sum("use").alias("Total use"),
    spark_sum("gen").alias("Total gen"),
    spark_sum("House overall").alias("Total House overall"),
    spark_sum("Dishwasher").alias("Total Dishwasher"),
    spark_sum("Furnace 1").alias("Total Furnace 1"),
    spark_sum("Furnace 2").alias("Total Furnace 2"),
    spark_sum("Home office").alias("Total Home office"),
    spark_sum("Fridge").alias("Total Fridge"),
    spark_sum("Wine cellar").alias("Total Wine cellar"),
    spark_sum("Garage door").alias("Total Garage door"),
    spark_sum("Kitchen 12").alias("Total Kitchen 12"),
    spark_sum("Kitchen 14").alias("Total Kitchen 14"),
    spark_sum("Kitchen 38").alias("Total Kitchen 38"),
    spark_sum("Barn").alias("Total Barn"),
    spark_sum("Well").alias("Total Well"),
    spark_sum("Microwave").alias("Total Microwave"),
    spark_sum("Living room").alias("Total Living room"),
    spark_sum("Solar").alias("Total Solar")
)

# Function to write to MySQL and update the cumulative sums
def write_to_mysql(df, epoch_id):
    if df.count() > 0:
        # Collect the data and prepare for insertion
        rows = df.collect()
        data_to_update = [(col, value) for row in rows for col, value in row.asDict().items()]

        # Connect to MySQL
        conn = mysql.connector.connect(
            host="database",
            user="root",
            password="123456",
            database="energy_consumption"
        )
        cursor = conn.cursor()

        # Create a dictionary to hold the current database state
        current_db_values = {}

        # Query to get all current data from the database
        cursor.execute("SELECT name, result FROM report")
        for db_name, db_result in cursor.fetchall():
            # Handle the case where `result` is NULL by defaulting to 0
            current_db_values[db_name] = db_result if db_result is not None else 0

        # Iterate through the new data and update or insert accordingly
        for name, new_sum in data_to_update:
            if name in current_db_values:
                # Update the existing record by adding the new value, considering if the current value is 0
                updated_value = current_db_values[name] + new_sum
                cursor.execute("UPDATE report SET result = %s WHERE name = %s", (updated_value, name))
            else:
                # Insert the new record if it doesn't exist
                cursor.execute("INSERT INTO report (name, result) VALUES (%s, %s)", (name, new_sum))

        conn.commit()
        cursor.close()
        conn.close()


# Write the streaming DataFrame to MySQL
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .start()

query.awaitTermination()
