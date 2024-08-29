import configparser

from pyspark import SparkConf


# Load the Spark Config
def load_spark_config(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read('conf/spark.conf')

    for (key, val) in config.items(env):
        spark_conf.set(key, val)

    return spark_conf


# Load the Application Config
def load_app_config(env):
    conf = dict()
    config = configparser.ConfigParser()
    config.read('conf/app.conf')

    for (key, val) in config.items(env):
        conf[key] = val

    return conf


def load_data_filter(env,key):
    conf = load_app_config(env)
    return True if conf[key] == "" else conf[key]
