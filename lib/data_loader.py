from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from lib.config_loader import load_app_config, load_data_filter


# Load Account Table
def load_account_table(spark: SparkSession, env, location=''):
    app_conf = load_app_config(env)
    runtime_filter = load_data_filter(env, 'party.filter')

    # Load from the hive table
    if app_conf['enable.hive'] is True:
        hive_db = app_conf['hive.database']
        return spark.sql(f'select * from {hive_db}.accounts where {runtime_filter}')
    else:
        return spark.read \
            .option('header', True) \
            .csv(location) \
            .where(runtime_filter)


# Load Party Table
def load_parties_table(spark: SparkSession, env, location=''):
    app_conf = load_app_config(env)
    runtime_filter = load_data_filter(env, 'address.filter')

    # Load from the hive table
    if app_conf['enable.hive'] is True:
        hive_db = app_conf['hive.database']
        return spark.sql(f'select * from {hive_db}.parties where {runtime_filter}')
    else:
        return spark.read \
            .option('header', True) \
            .csv(location) \
            .where(runtime_filter)


# Load Address Table
def load_address_table(spark: SparkSession, env, location=''):
    app_conf = load_app_config(env)
    runtime_filter = load_data_filter(env, 'address.filter')

    # Load from the hive table
    if app_conf['enable.hive'] is True:
        hive_db = app_conf['hive.database']
        return spark.sql(f'select * from {hive_db}.party_address where {runtime_filter}')
    else:
        return spark.read \
            .option('header', True) \
            .csv(location) \
            .where(runtime_filter)
