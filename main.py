import sys

from lib.logger import Log4J
from lib.transformations import get_contract, get_party, get_address, get_party_address_df, get_payload
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
    accounts_df_transformed = get_contract(accounts_df)

    parties_df = load_account_table(spark, env, 'test_data/parties/party_samples.csv')
    parties_df_transformed = get_party(parties_df)

    address_df = load_account_table(spark, env, 'test_data/party_address/address_samples.csv')
    address_df_transformed = get_address(address_df)

    party_addr_df = get_party_address_df(parties_df_transformed, address_df_transformed)
    get_payload(accounts_df_transformed, party_addr_df)
