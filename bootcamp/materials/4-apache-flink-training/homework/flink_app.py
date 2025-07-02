import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.window import Session
from pyflink.table.expressions import col, lit


def create_events_table(t_env):
    kafka_key = os.environ["KAFKA_WEB_TRAFFIC_KEY"]
    kafka_secret = os.environ["KAFKA_WEB_TRAFFIC_SECRET"]
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    ddl = f"""
        CREATE TABLE {table_name} (
            url STRING,
            referrer STRING,
            user_id BIGINT,
            device_id BIGINT,
            host STRING,
            ip STRING,
            event_time_str STRING,
            event_time AS TO_TIMESTAMP(event_time_str, '{pattern}'),
            WATERMARK FOR event_time AS event_time - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{os.environ["KAFKA_TOPIC"]}',
            'properties.bootstrap.servers' = '{os.environ["KAFKA_URL"]}',
            'properties.group.id' = '{os.environ["KAFKA_GROUP"]}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """
    t_env.execute_sql(ddl)
    return table_name


def log_session_stats():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_events_table(t_env)

    events = t_env.from_path(source_table)

    # Sessionize by ip and host
    sessions = events.window(
        Session.with_gap(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("ip"), col("host"), col("w")
    ).select(
        col("ip"),
        col("host"),
        col("w").start.alias("session_start"),
        col("w").end.alias("session_end"),
        col("url").count.alias("event_count")
    )

    # Average events per session on *.techcreator.io
    sessions.filter(col("host").like("%.techcreator.io")) \
        .group_by(col("host")) \
        .select(
            col("host"),
            col("event_count").avg.alias("avg_events_per_session")
        ) \
        .execute().print()

    # Compare specific hosts
    sessions.filter(
        col("host").in_(
            lit("zachwilson.techcreator.io"),
            lit("zachwilson.tech"),
            lit("lulu.techcreator.io")
        )
    ).group_by(col("host")) \
     .select(
         col("host"),
         col("event_count").avg.alias("avg_events_per_session")
     ) \
     .execute().print()


if __name__ == '__main__':
    log_session_stats()
