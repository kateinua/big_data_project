from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DoubleType

schema = StructType([
        StructField('venue', StructType([
            StructField("venue_name", StringType()),
            StructField("lon", DoubleType()),
            StructField("lat", DoubleType()),
            StructField("venue_id", DoubleType())
        ])),
        StructField("visibility", StringType()),
        StructField("response", StringType()),
        StructField("guests", LongType()),
        StructField('member', StructType([
            StructField("member_id", LongType()),
            StructField("photo", StringType()),
            StructField("member_name", StringType())
        ])),
        StructField("rsvp_id", LongType()),
        StructField("mtime", LongType()),
        StructField('event', StructType([
            StructField("event_name", StringType()),
            StructField("event_id", StringType()),
            StructField("time", LongType()),
            StructField("event_url", StringType())
        ])),
        StructField('group', StructType([
            StructField("group_topics", ArrayType(StructType([
                StructField("urlkey", StringType()),
                StructField("topic_name", StringType())
            ]))),
            StructField("group_city", StringType()),
            StructField("group_country", StringType()),
            StructField("group_id", LongType()),
            StructField("group_name", StringType()),
            StructField("group_lon", DoubleType()),
            StructField("group_lat", DoubleType()),
            StructField("group_urlname", StringType()),
            StructField("group_state", StringType())
        ]))
    ])
