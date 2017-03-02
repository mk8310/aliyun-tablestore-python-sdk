from ots2 import *
import time


'''
get row with max version
'''
def get_row_1(ots_client):
    primary_key = {'gid':1, 'uid':'500'}
    columns_to_get = [] 

    cond = CompositeCondition(LogicalOperator.OR)
    cond.add_sub_condition(RelationCondition("age", 10, ComparatorType.NOT_EQUAL))
    cond.add_sub_condition(RelationCondition("address", 'hangzhou', ComparatorType.EQUAL))

    consumed, primary_key_columns, attribute_columns = ots_client.get_row("python_sdk_4", primary_key, columns_to_get, None, 2, None)

    print primary_key_columns
    for attribute in attribute_columns:
        print "name:" + attribute.name + ", value:" + str(attribute.value) + ", timestamp:" + str(attribute.timestamp)

'''
get row with special timestamp
'''
def get_row_2(ots_client):
    primary_key = {'gid':1, 'uid':'200'}
    columns_to_get = ['name', 'address', 'age'] 

    cond = CompositeCondition(LogicalOperator.OR)
    cond.add_sub_condition(RelationCondition("age", 10, ComparatorType.NOT_EQUAL))
    cond.add_sub_condition(RelationCondition("address", 'hangzhou', ComparatorType.EQUAL))

    consumed, primary_key_columns, attribute_columns = ots_client.get_row("python_sdk_4", primary_key, columns_to_get, cond, None, 1487141236212)

    print primary_key_columns
    for attribute in attribute_columns:
        print "name:" + attribute.name + ", value:" + str(attribute.value) + ", timestamp:" + str(attribute.timestamp)


'''
get row with time range
'''
def get_row_3(ots_client):
    primary_key = {'gid':1, 'uid':'200'}
    columns_to_get = ['name', 'address', 'age'] # given a list of columns to get, or empty list if you want to get entire row.

    cond = CompositeCondition(LogicalOperator.OR)
    cond.add_sub_condition(RelationCondition("age", 10, ComparatorType.NOT_EQUAL))
    cond.add_sub_condition(RelationCondition("address", 'hangzhou', ComparatorType.EQUAL))

    consumed, primary_key_columns, attribute_columns = ots_client.get_row("python_sdk_4", primary_key, columns_to_get, None, None, (1487141236212, 1487142337252))

    print primary_key_columns
    for attribute in attribute_columns:
        print "name:" + attribute.name + ", value:" + str(attribute.value) + ", timestamp:" + str(attribute.timestamp)


if __name__ == '__main__':
    ots_client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)

    get_row_1(ots_client)


