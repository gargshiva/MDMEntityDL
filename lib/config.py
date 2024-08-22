import configparser

from pyspark import SparkConf


def load_spark_config(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read('conf/spark.conf')

    for (key, val) in config.items(env):
        spark_conf.set(key, val)

    return spark_conf
