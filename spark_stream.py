import logging
import os

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# connection settings, overridden by environment variables when running in Docker
SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')


def create_keyspace(session):
    # create the keyspace if it is not there already
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    # create the table that will hold the user records
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    # read each field from the incoming record
    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    # insert the row into Cassandra
    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address,
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        # start a Spark session and load the Kafka and Cassandra jars
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .master(SPARK_MASTER) \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
            .getOrCreate()

        # quieten the logs so only errors show
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # read the users_created topic as a stream, starting from the oldest message
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # point at the Cassandra cluster
        cluster = Cluster([CASSANDRA_HOST])

        # open a session
        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    # describe the shape of the incoming JSON
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # convert the Kafka message body to text, then split it into columns
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create the spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka and parse the messages
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        # connect to cassandra
        session = create_cassandra_connection()

        if session is not None:
            # set up the keyspace and table
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            # write the stream into the cassandra table
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            # keep running until stopped
            streaming_query.awaitTermination()
