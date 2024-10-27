from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes, FormatDescriptor)
from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf

# Create a Table environment
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().set("parallelism.default", "1")

# Define a Python UDF to split words
@udtf(result_types=[DataTypes.STRING()])
def split(line: Row):
    for s in line.split():
        yield Row(s)

# Register the Python UDF
t_env.register_function("split", split)

# Read a text file as a stream
t_env.execute_sql("""
    CREATE TABLE words (
        word STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = './data/words.csv',
        'format' = 'csv'
    )
""")

# Count the frequency of words
t_env.execute_sql("""
    SELECT worde, COUNT(*) AS `count`
    FROM words, LATERAL TABLE(split(word)) AS T(worde)
    GROUP BY worde
""").print()
