from pyspark.sql import SparkSession

from lib.config_loader import load_spark_config


class Utils:

    @classmethod
    def get_spark_session(cls, env):
        if env == 'LOCAL':
            spark_conf = load_spark_config(env)
            return SparkSession.builder \
                .config('spark.driver.extraJavaOptions',
                        '-Dlog4j2.configurationFile=log4j2.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark') \
                .config(conf=spark_conf) \
                .getOrCreate()
        else:
            return SparkSession.builder \
                .getOrCreate()
