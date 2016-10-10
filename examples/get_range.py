# -*- coding: utf8 -*-

from example_config import *
from ots2 import *
import time

table_name = 'OTSGetRangeSimpleExample'

def create_table(ots_client):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    ots_client.create_table(table_meta, reserved_throughput)
    print 'Table has been created.'

def delete_table(ots_client):
    ots_client.delete_table(table_name)
    print 'Table \'%s\' has been deleted.' % table_name

def put_row(ots_client):
    for i in range(0, 100):
        primary_key = {'gid':i, 'uid':i+1}
        attribute_columns = {'name':'John', 'mobile':i, 'address':'China', 'age':i}
        condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST) # Expect not exist: put it into table only when this row is not exist.
        consumed = ots_client.put_row(table_name, condition, primary_key, attribute_columns)
        print u'Write succeed, consume %s write cu.' % consumed.write

def get_range(ots_client): 
    '''
        Scan table to get all the rows.
        It will not return you all once, you should continue read from next start primary key till next start primary key is None.
    '''
    inclusive_start_primary_key = {'gid':INF_MIN, 'uid':INF_MIN} 
    exclusive_end_primary_key = {'gid':INF_MAX, 'uid':INF_MAX} 
    columns_to_get = []

    cond = CompositeCondition(LogicalOperator.AND)
    cond.add_sub_condition(RelationCondition("address", 'China', ComparatorType.EQUAL))
    cond.add_sub_condition(RelationCondition("age", 50, ComparatorType.LESS_THAN))

    consumed, next_start_primary_key, row_list = ots_client.get_range(
                table_name, Direction.FORWARD, 
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get, 10, 
                column_filter = cond
    )

    all_rows = []
    all_rows.extend(row_list)
    while next_start_primary_key is not None:
        inclusive_start_primary_key = next_start_primary_key
        consumed, next_start_primary_key, row_list = ots_client.get_range(
                table_name, Direction.FORWARD, 
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get, 10, 
                column_filter = cond
        )
        all_rows.extend(row_list)
        print 'Read succeed, consume %s read cu.' % consumed.read

    for row in all_rows:
        print row
    print 'Total rows: ', len(all_rows)

def xget_range(ots_client):
    '''
        You can easily scan the range use xget_range, without handling next start primary key.
    '''
    consumed_counter = CapacityUnit(0, 0)
    inclusive_start_primary_key = {'gid':INF_MIN, 'uid':INF_MIN} 
    exclusive_end_primary_key = {'gid':INF_MAX, 'uid':INF_MAX}

    cond = CompositeCondition(LogicalOperator.AND)
    cond.add_sub_condition(RelationCondition("address", 'China', ComparatorType.EQUAL))
    cond.add_sub_condition(RelationCondition("age", 50, ComparatorType.GREATER_EQUAL))

     
    columns_to_get = []
    range_iter = ots_client.xget_range(
                table_name, Direction.FORWARD, 
                inclusive_start_primary_key, exclusive_end_primary_key,
                consumed_counter, columns_to_get, 100,
                column_filter = cond
    )

    total_rows = 0
    for (primary_key_columns, attribute_columns) in range_iter:
        print primary_key_columns, attribute_columns
        total_rows += 1

    print 'Total rows:', total_rows

if __name__ == '__main__':
    ots_client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(ots_client)
    except:
        pass
    create_table(ots_client)

    time.sleep(3) # wait for table ready
    put_row(ots_client)
    get_range(ots_client)
    xget_range(ots_client)
    delete_table(ots_client)

