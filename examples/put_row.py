# -*- coding: utf8 -*-

from example_config import *

from ots2 import *
import time

table_name = 'OTSPutRowSimpleExample'

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
    primary_key = {'gid':1, 'uid':101}
    attribute_columns = {'name':'John', 'mobile':15100000000, 'address':'China', 'age':20}

    # Expect not exist: put it into table only when this row is not exist.
    condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)
    consumed = ots_client.put_row(table_name, condition, primary_key, attribute_columns)
    print u'Write succeed, consume %s write cu.' % consumed.write

    attribute_columns = {'name':'John', 'mobile':15100000000, 'address':'China', 'age':25}
    condition = Condition(RowExistenceExpectation.EXPECT_EXIST, RelationCondition("age", 20, ComparatorType.EQUAL))
    consumed = ots_client.put_row(table_name, condition, primary_key, attribute_columns)
    print u'Write succeed, consume %s write cu.' % consumed.write

    attribute_columns = {'name':'John', 'mobile':15100000000, 'address':'China', 'age':25}

    # 上面的age已经被修改为25了，现在我们继续期望age=20，TableStore将报错
    condition = Condition(RowExistenceExpectation.EXPECT_EXIST, RelationCondition("age", 20, ComparatorType.EQUAL))
    try:
        consumed = ots_client.put_row(table_name, condition, primary_key, attribute_columns)
    except OTSServiceError, e:
        print str(e)

if __name__ == '__main__':
    ots_client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    create_table(ots_client)

    time.sleep(3) # wait for table ready
    put_row(ots_client)
    delete_table(ots_client)

