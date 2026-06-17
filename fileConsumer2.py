from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, BooleanType, DoubleType
from pyspark.sql.functions import col, explode, from_json

STATE_FIELDS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]

NUMERIC_CASTS = {
    "time_position": LongType(),
    "last_contact": LongType(),
    "longitude": DoubleType(),
    "latitude": DoubleType(),
    "baro_altitude": DoubleType(),
    "on_ground": BooleanType(),
    "velocity": DoubleType(),
    "true_track": DoubleType(),
    "vertical_rate": DoubleType(),
    "geo_altitude": DoubleType(),
}

spark = (
    SparkSession.builder.appName("OpenSkyStream")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2")
    .getOrCreate()
)

# 2. Read from Kafka
topicKafka = "myTopic" # Replace with your Kafka topic name

schema = StructType([
    StructField("ingest_ts", StringType(), True),
    StructField("api_ts", LongType(), True),
    StructField("states", ArrayType(ArrayType(StringType(), True), True), True),
])

stream_df = (
    spark.readStream.format("kafka")
    #.option("kafka.bootstrap.servers", "localhost:9092")
    #.option("kafka.bootstrap.servers", "172.17.0.1:9092") 
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", topicKafka)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_payload = stream_df.select(
    from_json(col("value").cast("string"), schema).alias("payload")
).select("payload.*")

parsed_states = (
    parsed_payload
    .withColumn("state", explode(col("states")))
    .select(
        "ingest_ts",
        "api_ts",
        *[
            (
                col("state").getItem(idx).cast(NUMERIC_CASTS[field])
                if field in NUMERIC_CASTS
                else col("state").getItem(idx)
            ).alias(field)
            for idx, field in enumerate(STATE_FIELDS)
        ]
    )
)

parsed_states.printSchema()

# Here edit code to include the required functionalities

# 1-Group by origin_country and count the number of flights per country


# 2- get the average velocity of flights per country not on the ground (on_ground = false)


# 3 - Get the number of flights  for country of Portugal (origin_country = "Portugal") and the average velocity of  flights not on the ground (on_ground = false)



query = (
    parsed_states.writeStream
    .format("console")
    .outputMode("append")
    #.option("checkpointLocation", "checkpoints/opensky_stream")
    .start()
)

query.awaitTermination()
