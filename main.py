import sys

from lib.logger import Log4J
from lib.transformations import get_contract, get_party, get_party_address
from lib.utils import Utils
from lib.data_loader import load_account_table

# Main method
# Create Spark Session
if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Usage: Runtime args are missing !')
        sys.exit(-1)

    env = sys.argv[1].upper()
    spark = Utils.get_spark_session(env)
    logger = Log4J(spark)

    logger.info(spark.sparkContext.getConf().toDebugString())

    accounts_df = load_account_table(spark, env, 'test_data/accounts/account_samples.csv')
    parties_df = load_account_table(spark, env, 'test_data/parties/party_samples.csv')
    party_address_df = load_account_table(spark, env, 'test_data/party_address/address_samples.csv')
    party_address_df.show(truncate=False)
    get_party_address(party_address_df).show(truncate=False)
    get_party_address(party_address_df).printSchema()
