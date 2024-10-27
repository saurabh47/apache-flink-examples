from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col, lit

# Create a TableEnvironment in streaming mode
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Add the Kafka connector JAR to the classpath so that the Flink job can interact with Kafka topics.
# Download connector jar from https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka/3.2.0-1.19
table_env.get_config().set("pipeline.jars", "file:///path_to_connector/flink-sql-connector-kafka-3.2.0-1.19.jar")
table_env.get_config().get_configuration().set_integer('parallelism.default', 1)
table_env.get_config().get_configuration().set_integer('taskmanager.numberOfTaskSlots', 4)

# Define the source table: Simulating stock price ticks from Kafka
table_env.execute_sql("""
    CREATE TABLE stock_prices (
        stock_symbol STRING,
        price DOUBLE,
        event_time TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'stock_prices_topic',
        'properties.bootstrap.servers' = '{{properties.bootstrap.servers}}',
        'properties.advertised.listeners' = '{{properties.advertised.listeners}}',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'PLAIN',
        'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{{username}}" password="{{password}}";',
        'properties.group.id' = 'flink_py_job',
        'json.timestamp-format.standard' = 'ISO-8601',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
""")

# Define the sink table: Output buy/sell signals to Kafka
table_env.execute_sql("""
    CREATE TABLE stock_signals (
        stock_symbol STRING,
        signal STRING,  -- 'BUY' or 'SELL'
        price DOUBLE,
        event_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'stock_signals_topic',
        'properties.bootstrap.servers' = '{{properties.bootstrap.servers}}',
        'properties.advertised.listeners' = '{{properties.advertised.listeners}}',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'PLAIN',
        'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{{username}}" password="{{password}}";',
        'properties.group.id' = 'flink_py_job',
        'json.timestamp-format.standard' = 'ISO-8601',
        'format' = 'json'
    )
""")

# Processing Logic: Generate buy/sell signals based on percentage price changes
table_env.execute_sql("""
    INSERT INTO stock_signals
    SELECT * 
    FROM(
        SELECT 
            stock_symbol,
            CASE
                WHEN (price / LAG(price, 1) OVER (PARTITION BY stock_symbol ORDER BY event_time) < 0.95) THEN 'BUY'
                WHEN (price / LAG(price, 1) OVER (PARTITION BY stock_symbol ORDER BY event_time) > 1.05) THEN 'SELL'
                ELSE NULL
            END as signal,
            price,
            event_time
        FROM stock_prices
    )                  
    WHERE signal is not NULL
""").wait()
