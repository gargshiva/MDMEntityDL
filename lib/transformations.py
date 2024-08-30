from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import struct, col, lit, array, when, isnull, filter, collect_list


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
        col('account_id'),
        get_insert_operation(col('party_id'), 'partyIdentifier'),
        get_insert_operation(col('relation_type'), 'partyRelationshipType'),
        get_insert_operation(col('relation_start_date'), 'partyRelationStartDateTime')
    )


# Transform the dataframe fields into struct {Operation,NewValue,OldValue}
def get_address(address_df: DataFrame):
    addr = struct(
        address_df['address_line_1'].alias('addressLine1'),
        address_df['address_line_2'].alias('addressLine2'),
        address_df['city'].alias('addressCity'),
        address_df['postal_code'].alias('addressPostalCode'),
        address_df['country_of_address'].alias('addressCountry'),
        address_df['address_start_date'].alias('addressStartDate'),
    )
    return address_df.select(col('party_id'), get_insert_operation(addr, 'partyAddress'))


def get_party_address_df(party_df: DataFrame, address_df: DataFrame):
    party_address_df = party_df.join(address_df,
                                     party_df['party_id'] == address_df['party_id'],
                                     'left_outer').drop(address_df['party_id']).drop(party_df['party_id'])

    party_relations = struct(
        party_address_df['partyIdentifier'],
        party_address_df['partyRelationshipType'],
        party_address_df['partyRelationStartDateTime'],
        party_address_df['partyAddress']
    ).alias('party_relations')

    party_relations_df = party_address_df \
        .select('account_id', party_relations) \
        .groupby('account_id').agg(collect_list('party_relations').alias('partyRelations'))

    return party_relations_df


def get_payload(contract_df: DataFrame, party_address_df: DataFrame):
    payload_df = contract_df.join(party_address_df,
                                  contract_df['account_id'] == party_address_df['account_id'],
                                  'left_outer')

    payload = struct(
        payload_df['contractIdentifier'],
        payload_df['sourceSystemIdentifier'],
        payload_df['contactStartDateTime'],
        payload_df['contractTitle'],
        payload_df['taxIdentifier'],
        payload_df['contractBranchCode'],
        payload_df['contractCountry'],
        payload_df['partyRelations']
    ).alias('payload')
    payload_df.select(payload).show(truncate=False)
    payload_df.select(payload).printSchema()

    payload_df.select(payload).write.json('op')
