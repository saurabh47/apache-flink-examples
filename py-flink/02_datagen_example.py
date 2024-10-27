from pyflink.table import EnvironmentSettings, TableEnvironment

# create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# create source Table using DataGen connector 
table_env.execute_sql("""
    CREATE TABLE source_table (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '10'
    )
""")

# create sink Table
table_env.execute_sql("""
    CREATE TABLE sink_table (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'print'
    )
""")

#Insert result into sink Table
table_env.execute_sql("""
    INSERT INTO sink_table
    SELECT id + 1, data FROM source_table                  
""")