from pyflink.table import EnvironmentSettings, TableEnvironment

# create a batch TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1760, 'TCS'), (788, 'INFY'), (1760, 'TCS')], ['price', 'script'])

table.execute().print()