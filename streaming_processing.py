from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, from_json, to_json, udf, struct
import os
from schema import schema

SERVERS = os.environ.get('SERVERS')
topic = 'meetups'


def write_to_cassandra(df, table):
    df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=topic, table=table) \
        .start()


spark_session = SparkSession.builder.appName("Meetups") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

spark_session.conf.set("spark.sql.streaming.checkpointLocation", '.')

df = spark_session.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", SERVERS) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn('value', from_json('value', schema)) \
    .selectExpr('value.*')

cities = df.select(
    col('group.group_country').alias('country'),
    col('group.group_city').alias('city')
)

events = df.select(
    col('event.event_id').alias('event_id'),
    col('event.event_name').alias('event_name'),
    col('event.time').alias('event_time'),
    col('group.group_topics.topic_name').alias('topics'),
    col('group.group_name').alias('group_name'),
    col('group.group_country').alias('country'),
    col('group.group_city').alias('city'),
)

city_groups = df.select(
    col('group.group_city').alias('city_name'),
    col('group.group_name').alias('group_name'),
    col('group.group_id').alias('group_id'),
)

groups = df.select(
    col('group.group_id').alias('group_id'),
    col('event.event_id').alias('event_id'),
    col('event.event_name').alias('event_name'),
    col('event.time').alias('event_time'),
    col('group.group_topics.topic_name').alias('topics'),
    col('group.group_name').alias('group_name'),
    col('group.group_country').alias('country'),
    col('group.group_city').alias('city'),
)

write_to_cassandra(cities, 'countries')
write_to_cassandra(events, 'events')
write_to_cassandra(city_groups, 'city_groups')
write_to_cassandra(groups, 'group_events')

spark_session.streams.awaitAnyTermination()
