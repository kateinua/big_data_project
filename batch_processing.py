import os
from pyspark.sql import SparkSession
from pyspark.sql import col, from_json, explode, lit, array, count, hour, first, to_json, udf, collect_list, struct, \
    collect_set, row_number, desc
from pyspark.sql.types import StringType, ArrayType, TimestampType
from datetime import datetime, timedelta
from pyspark.sql import Window

from schema import schema

SERVERS = os.environ.get('SERVERS')
STATES = "./states.json"


def get_topics(group_topics):
    topics = [topic_["topic_name"] for topic_ in group_topics]
    for topic_ in group_topics:
        topics.append(topic_["topic_name"])
    return topics


def write_answers(task, table):
    task.write.format("org.apache.spark.sql.cassandra").options(keyspace='meetups', table=table).mode('append').save()


if __name__ == "__main__":
    topic = 'meetups'
    spark = SparkSession.builder.appName("Meetups").getOrCreate()

    states = spark.read.json(STATES)

    events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", SERVERS) \
        .option('topic', topic) \
        .option("subscribe", topic) \
        .load()

    time_now = datetime.now()
    diff = timedelta(minutes=time_now.minute, seconds=time_now.second, microseconds=time_now.microsecond)
    upper = time_now - diff

    events = events.select(col("key").cast("string"), col("timestamp").cast(TimestampType()),
                           from_json(col("value").cast("string"), schema).alias('value')) \
        .selectExpr('timestamp', ' value.*').withColumn('time', lit(time_now.hour)) \
        .withColumn("group_topics", udf(get_topics, ArrayType(StringType()))("group.group_topics"))

    batch1 = upper - timedelta(hours=6)
    task1 = events.where((events["time"] > batch1) & (events["time"] <= upper)) \
        .groupby("group.group_country").agg(count("*").alias("count"))
    batch2 = upper - timedelta(hours=3)
    task2 = events.where((events["time"] > batch2) & (events["time"] < upper)) \
        .where(col("group.group_country") == "us").join(states, col("group.group_state") == states["code"]).groupby(
        "state_name").agg(collect_set("group.group_name").alias("groups"))
    batch3 = upper - timedelta(hours=6)
    task3 = events \
        .where((events["time"] > batch3) & (events["time"] < upper)) \
        .withColumn("topic", explode("group_topics")) \
        .groupby("group.group_country", "topic") \
        .agg(count("*").alias("count")) \
        .withColumn("rank", row_number().over(Window.partitionBy("group_country").orderBy(desc("count")))) \
        .where(col("rank") == 1).drop("rank")

    write_answers(task1, "events_data")
    write_answers(task2, "groups_data")
    write_answers(task3, "topics_data")
