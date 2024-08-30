from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import struct, col, lit, array, when, isnull, filter


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
    tax_identifier = struct(
        accounts_df['tax_id_type'].alias('taxIdType'),
        accounts_df['tax_id'].alias('taxId')
    )

    # Array of structs
    contract_title = array(when(~isnull('legal_title_1'),
                                struct(
                                    lit('lgl_ttl_ln_1').alias('contractTitleLineType'),
                                    accounts_df['legal_title_1'].alias('contractTitleLine')
                                )),
                           when(~isnull('legal_title_2'),
                                struct(
                                    lit('lgl_ttl_ln_2').alias('contractTitleLineType'),
                                    accounts_df['legal_title_2'].alias('contractTitleLine')
                                )),
                           )

    contract_title_nl = filter(contract_title, lambda x: ~isnull(x))

    return accounts_df.select(
        col('account_id'),
        get_insert_operation(col('account_id'), 'contractIdentifier'),
        get_insert_operation(col('source_sys'), 'sourceSystemIdentifier'),
        get_insert_operation(col('account_start_date'), 'contactStartDateTime'),
        get_insert_operation(col('branch_code'), 'contractBranchCode'),
        get_insert_operation(col('country'), 'contractCountry'),
        get_insert_operation(tax_identifier, 'taxIdentifier'),
        get_insert_operation(contract_title_nl, 'contractTitle')
    )

# Transform the dataframe fields into struct {Operation,NewValue,OldValue}
def get_party(party_df: DataFrame):
    return party_df.select(
        col('party_id'),
        get_insert_operation(col('party_id'), 'partyIdentifier'),
        get_insert_operation(col('relation_type'), 'partyRelationshipType'),
        get_insert_operation(col('relation_start_date'), 'partyRelationStartDateTime')
    )

# Transform the dataframe fields into struct {Operation,NewValue,OldValue}
def get_party_address(party_address_df: DataFrame):
    addr = struct(
        party_address_df['address_line_1'].alias('addressLine1'),
        party_address_df['address_line_2'].alias('addressLine2'),
        party_address_df['city'].alias('addressCity'),
        party_address_df['postal_code'].alias('addressPostalCode'),
        party_address_df['country_of_address'].alias('addressCountry'),
        party_address_df['address_start_date'].alias('addressStartDate'),
    )
    return party_address_df.select(get_insert_operation(addr, 'partyAddress'))
