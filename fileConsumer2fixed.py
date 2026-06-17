from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, BooleanType, DoubleType
from pyspark.sql.functions import col, explode

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

spark = SparkSession.builder.appName("OpenSkyStream").getOrCreate()

schema = StructType([
    StructField("ingest_ts", StringType(), True),
    StructField("api_ts", LongType(), True),
    StructField("states", ArrayType(ArrayType(StringType(), True), True), True),
])

stream_df = (
    spark.readStream
    .schema(schema)
    .json("data/opensky_raw")
)

parsed_states = (
    stream_df
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

# print out type of velocity column
print("Velocity column type:", parsed_states.schema["velocity"].dataType)
# print out type of on_ground column
print("On_ground column type:", parsed_states.schema["on_ground"].dataType)

# 1-Group by origin_country and count the number of flights per country in descending order
flights_by_country = parsed_states.groupBy("origin_country").count().orderBy(col("count").desc())

# 2- get the average velocity of flights per country not on the ground (on_ground = false)
avg_velocity_by_country = (
    parsed_states
    .filter(col("on_ground") == False)
    .groupBy("origin_country")
    .avg("velocity")
)

# 3 - Get the number of flights  for country of Portugal (origin_country = "Portugal") and the 
# maximum velocity of  flights not on the ground (on_ground = false)
portugal_flights = (
    parsed_states
    .filter(col("origin_country") == "Portugal")
    .groupBy("origin_country")
    .count()
)

portugal_max_velocity = (
    parsed_states
    .filter((col("origin_country") == "Portugal") & (col("on_ground") == False))
    .groupBy("origin_country")
    .max("velocity")
)

# max velocity for all countries sorted in descending order
max_velocity_by_country = (
    parsed_states
    .filter(col("on_ground") == False)
    .groupBy("origin_country")
    .max("velocity")
    .orderBy(col("max(velocity)").desc())
)

query = (
    parsed_states.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "checkpoints/opensky_stream")
    .start()
)

flights_by_country_query = (
    flights_by_country.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "checkpoints/flights_by_country")
    .start()
)

avg_velocity_query = (
    avg_velocity_by_country.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "checkpoints/avg_velocity_by_country")
    .start()
)

portugal_flights_query = (
    portugal_flights.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "checkpoints/portugal_flights")
    .start()
)

portugal_max_velocity_query = (
    portugal_max_velocity.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "checkpoints/portugal_max_velocity")
    .start()
)

max_velocity_by_country_query = (
    max_velocity_by_country.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "checkpoints/max_velocity_by_country")    
    .start()
)

query.awaitTermination()
