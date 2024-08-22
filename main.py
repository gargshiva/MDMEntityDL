import sys
from lib.logger import Log4J
from lib.utils import Utils

# Main method
if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Usage: Runtime args are missing !')
        sys.exit(-1)

    env = sys.argv[1].upper()
    spark = Utils.get_spark_session(env)
    logger = Log4J(spark)

    logger.info(spark.sparkContext.getConf().toDebugString())
