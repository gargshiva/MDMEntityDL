from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import struct, col, lit


# Every Column is represented as struct
# Struct Schema {Operation , NewValue ,  OldValue}
def get_insert_operation(column, alias) -> Column:
    return struct(
        lit('Insert').alias('Operation'),
        column.alias('newValue'),
        lit(None).alias('oldValue')
    ).alias(alias)


# Transform the dataframe fields into struct {Operation,NewValue,OldValue}
def get_contract(accounts_df: DataFrame) -> DataFrame:
    return accounts_df.select(
        get_insert_operation(col('account_id'), 'contractIdentifier'),
        get_insert_operation(col('source_sys'), 'sourceSystemIdentifier'),
        get_insert_operation(col('account_start_date'), 'contactStartDateTime'),
        get_insert_operation(col('branch_code'), 'contractBranchCode'),
        get_insert_operation(col('country'), 'contractCountry'),
    )
