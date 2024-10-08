import sys

from lib.logger import Log4J
from lib.transformations import get_contract, get_party, get_address, get_party_address_df, get_payload, apply_header
from lib.utils import Utils
from lib.data_loader import load_account_table

from pyspark.sql import DataFrame

# Main method
# Create Spark Session
if __name__ == '__main__':

    if len(sys.argv) < 6:
        print('Usage: Runtime args are missing - Env , Accounts, Parties, Address DS , Output!')
        sys.exit(-1)

    env = sys.argv[1].upper()
    spark = Utils.get_spark_session(env)
    logger = Log4J(spark)

    account_ds_location = sys.argv[2]
    parties_ds_location = sys.argv[3]
    parties_add_ds_location = sys.argv[4]
    output_location = sys.argv[5]

   # logger.info(spark.sparkContext.getConf().toDebugString())

    logger.info(f'Environment -> {env}')
    logger.info(f'Accounts DS -> {account_ds_location}')
    logger.info(f'Parties DS -> {parties_ds_location}')
    logger.info(f'Parties Address DS -> {parties_add_ds_location}')
    logger.info(f'Output Location -> {output_location}')

    # Accounts DF
    accounts_df = load_account_table(spark, env, account_ds_location)
    accounts_df_transformed = get_contract(accounts_df)

    # Parties DF
    parties_df = load_account_table(spark, env, parties_ds_location)
    parties_df_transformed = get_party(parties_df)

    # Address DF
    address_df = load_account_table(spark, env, parties_add_ds_location)
    address_df_transformed = get_address(address_df)

    party_addr_df = get_party_address_df(parties_df_transformed, address_df_transformed)
    payload_df = get_payload(accounts_df_transformed, party_addr_df)
    event_df: DataFrame = apply_header(payload_df, spark).repartition(1).cache()
    event_df.show(truncate=False)
    event_df.write.mode("overwrite").json(output_location)
