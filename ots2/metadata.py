# -*- coding: utf8 -*-

from ots2.error import *

__all__ = [
    'INF_MIN',
    'INF_MAX',
    'TableMeta',
    'CapacityUnit',
    'ReservedThroughput',
    'ReservedThroughputDetails',
    'ColumnType',
    'Direction',
    'UpdateTableResponse',
    'DescribeTableResponse',
    'RowDataItem',
    'Condition',
    'PutRowItem',
    'UpdateRowItem',
    'DeleteRowItem',
    'MultiTableInBatchGetRowItem',
    'TableInBatchGetRowItem',
    'MultiTableInBatchGetRowResult',
    'BatchWriteRowType',
    'MultiTableInBatchWriteRowItem',
    'TableInBatchWriteRowItem',
    'MultiTableInBatchWriteRowResult',
    'BatchWriteRowResponseItem',
    'LogicalOperator',
    'ComparatorType',
    'ColumnConditionType',
    'ColumnCondition',
    'CompositeCondition',
    'RelationCondition',
    'RowExistenceExpectation', 
]


class TableMeta(object):

    def __init__(self, table_name, schema_of_primary_key):
        # schema_of_primary_key: [('PK0', 'STRING'), ('PK1', 'INTEGER'), ...]
        self.table_name = table_name
        self.schema_of_primary_key = schema_of_primary_key


class CapacityUnit(object):

    def __init__(self, read=0, write=0):
        self.read = read
        self.write = write


class ReservedThroughput(object):

    def __init__(self, capacity_unit):
        self.capacity_unit = capacity_unit


class ReservedThroughputDetails(object):
    
    def __init__(self, capacity_unit, last_increase_time, last_decrease_time, number_of_decreases_today):
        self.capacity_unit = capacity_unit
        self.last_increase_time = last_increase_time
        self.last_decrease_time = last_decrease_time
        self.number_of_decreases_today = number_of_decreases_today

class ColumnType(object):
    STRING = "STRING"
    INTEGER = "INTEGER"
    BOOLEAN = "BOOLEAN"
    DOUBLE = "DOUBLE"
    BINARY = "BINARY"
    INF_MIN = "INF_MIN"
    INF_MAX = "INF_MAX"


class Direction(object):
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"

class UpdateTableResponse(object):

    def __init__(self, reserved_throughput_details):
        self.reserved_throughput_details = reserved_throughput_details


class DescribeTableResponse(object):

    def __init__(self, table_meta, reserved_throughput_details):
        self.table_meta = table_meta
        self.reserved_throughput_details = reserved_throughput_details


class RowDataItem(object):

    def __init__(self, is_ok, error_code, error_message, table_name, consumed, primary_key_columns, attribute_columns):
        # is_ok can be True or False
        # when is_ok is False,
        #     error_code & error_message are available
        # when is_ok is True,
        #     consumed & primary_key_columns & attribute_columns are available
        self.is_ok = is_ok
        self.error_code = error_code
        self.error_message = error_message
        self.table_name = table_name
        self.consumed = consumed
        self.primary_key_columns = primary_key_columns
        self.attribute_columns = attribute_columns

class LogicalOperator(object):
    NOT = 0
    AND = 1
    OR = 2

    __values__ = [
        NOT,
        AND,
        OR
    ]

    __members__ = [
        "LogicalOperator.NOT",
        "LogicalOperator.AND",
        "LogicalOperator.OR"
    ]

class ComparatorType(object):
    EQUAL = 0
    NOT_EQUAL = 1
    GREATER_THAN = 2
    GREATER_EQUAL = 3
    LESS_THAN = 4
    LESS_EQUAL = 5

    __values__ = [
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_EQUAL,
        LESS_THAN,
        LESS_EQUAL,
    ]

    __members__ = [
        "ComparatorType.EQUAL",
        "ComparatorType.NOT_EQUAL",
        "ComparatorType.GREATER_THAN",
        "ComparatorType.GREATER_EQUAL",
        "ComparatorType.LESS_THAN",
        "ComparatorType.LESS_EQUAL",
    ]


class ColumnConditionType(object):
    COMPOSITE_CONDITION = 0
    RELATION_CONDITION = 1

class ColumnCondition(object):
    pass    

class CompositeCondition(ColumnCondition):
    
    def __init__(self, combinator):
        self.sub_conditions = []
        self.set_combinator(combinator)

    def get_type(self):
        return ColumnConditionType.COMPOSITE_CONDITION

    def set_combinator(self, combinator):
        if combinator not in LogicalOperator.__values__:
            raise OTSClientError(
                "Expect input combinator should be one of %s, but '%s'"%(str(LogicalOperator.__members__), combinator)
            )
        self.combinator = combinator

    def get_combinator(self):
        return combinator

    def add_sub_condition(self, condition):
        if not isinstance(condition, ColumnCondition):
            raise OTSClientError(
                "The input condition should be an instance of ColumnCondition, not %s"%
                condition.__class__.__name__
            )
 
        self.sub_conditions.append(condition)

    def clear_sub_condition(self):
        self.sub_conditions = []

class RelationCondition(ColumnCondition):
   
    def __init__(self, column_name, column_value, comparator, pass_if_missing = True):
        self.column_name = column_name
        self.column_value = column_value

        self.comparator = None
        self.pass_if_missing = None

        self.set_comparator(comparator)
        self.set_pass_if_missing(pass_if_missing)

    def get_type(self):
        return ColumnConditionType.RELATION_CONDITION

    def set_pass_if_missing(self, pass_if_missing):
        """
        设置```pass_if_missing```

        由于OTS一行的属性列不固定，有可能存在有condition条件的列在该行不存在的情况，这时
        参数控制在这种情况下对该行的检查结果。
        如果设置为True，则若列在该行中不存在，则检查条件通过。
        如果设置为False，则若列在该行中不存在，则检查条件失败。
        默认值为True。
        """
        if not isinstance(pass_if_missing, bool):
            raise OTSClientError(
                "The input pass_if_missing should be an instance of Bool, not %s"%
                pass_if_missing.__class__.__name__
            )
        self.pass_if_missing = pass_if_missing

    def get_pass_if_missing(self):
        return self.pass_if_missing

    def set_column_name(self, column_name):
        self.column_name = column_name

    def get_column_name(self):
        return self.column_name

    def set_column_value(self, column_value):
        self.column_value = column_value

    def get_column_value(self):
        return self.column_value

    def set_comparator(self, comparator):
        if comparator not in ComparatorType.__values__:
            raise OTSClientError(
                "Expect input comparator should be one of %s, but '%s'"%(str(ComparatorType.__members__), comparator)
            )
        self.comparator = comparator

    def get_comparator(self):
        return self.comparator

class RowExistenceExpectation(object):
    IGNORE = "IGNORE"
    EXPECT_EXIST = "EXPECT_EXIST"
    EXPECT_NOT_EXIST = "EXPECT_NOT_EXIST"

    __values__ = [
        IGNORE,
        EXPECT_EXIST,
        EXPECT_NOT_EXIST,
    ]

    __members__ = [
        "RowExistenceExpectation.IGNORE",
        "RowExistenceExpectation.EXPECT_EXIST",
        "RowExistenceExpectation.EXPECT_NOT_EXIST",
    ]

class Condition(object):

    def __init__(self, row_existence_expectation, column_condition = None):
        self.row_existence_expectation = None
        self.column_condition = None

        self.set_row_existence_expectation(row_existence_expectation)
        if column_condition != None:
            self.set_column_condition(column_condition)

    def set_row_existence_expectation(self, row_existence_expectation):
        if row_existence_expectation not in RowExistenceExpectation.__values__:
            raise OTSClientError(
                "Expect input row_existence_expectation should be one of %s, but '%s'"%(str(RowExistenceExpectation.__members__), row_existence_expectation)
            )

        self.row_existence_expectation = row_existence_expectation
        
    def get_row_existence_expectation(self):
        return self.row_existence_expectation 

    def set_column_condition(self, column_condition):
        if not isinstance(column_condition, ColumnCondition):
            raise OTSClientError(
                "The input column_condition should be an instance of ColumnCondition, not %s"%
                column_condition.__class__.__name__
            )
        self.column_condition = column_condition

    def get_column_condition(self):
        self.column_condition

class PutRowItem(object):

    def __init__(self, condition, primary_key, attribute_columns):
        self.condition = condition
        self.primary_key = primary_key
        self.attribute_columns = attribute_columns


class UpdateRowItem(object):
    
    def __init__(self, condition, primary_key, update_of_attribute_columns):
        self.condition = condition
        self.primary_key = primary_key
        self.update_of_attribute_columns = update_of_attribute_columns


class DeleteRowItem(object):
    
    def __init__(self, condition, primary_key):
        self.condition = condition
        self.primary_key = primary_key


class TableInBatchGetRowItem(object):

    def __init__(self, table_name, primary_keys, columns_to_get=None, column_filter=None):
        self.table_name = table_name
        self.primary_keys = primary_keys
        self.columns_to_get = columns_to_get
        self.column_filter = column_filter


class MultiTableInBatchGetRowItem(object):

    def __init__(self):
        self.items = {}

    def add(self, table_item):
        """
        说明：添加ots2.metadata.TableInBatchGetRowItem对象
        注意：对象内部存储ots2.metadata.TableInBatchGetRowItem对象采用‘字典’的形式，Key是表
              的名字，因此如果插入同表名的对象，那么之前的对象将被覆盖。
        """
        if not isinstance(table_item, TableInBatchGetRowItem):
            raise OTSClientError(
                "The input table_item should be an instance of TableInBatchGetRowItem, not %s"%
                table_item.__class__.__name__
            )

        self.items[table_item.table_name] = table_item

class MultiTableInBatchGetRowResult(object):

    def __init__(self, response):
        self.items = {}

        for rows in response:
            for row in rows:
                table_name = row.table_name
                result_rows = self.items.get(table_name)
                if result_rows == None:
                    self.items[table_name] = [row]
                else:
                    result_rows.append(row)

    def get_failed_rows(self):
        succ, fail = self.get_result()
        return fail

    def get_succeed_rows(self):
        succ, fail = self.get_result()
        return succ

    def get_result(self):
        succ = []
        fail = []
        for rows in self.items.values():
            for row in rows:
                if row.is_ok:
                    succ.append(row)
                else:
                    fail.append(row)

        return succ, fail

    def get_result_by_table(self, table_name):
        return self.items.get(table_name)

    def is_all_succeed(self):
        return len(self.get_failed_rows()) == 0

class BatchWriteRowType(object):
    PUT = "put"
    UPDATE = "update"
    DELETE = "delete"


class TableInBatchWriteRowItem(object):
    
    def __init__(self, table_name, put=None, update=None, delete=None):
        self.table_name = table_name
        self.put = put
        self.update = update
        self.delete = delete


class MultiTableInBatchWriteRowItem(object):

    def __init__(self):
        self.items = {}

    def add(self, table_item):
        """
        说明：添加ots2.metadata.TableInBatchWriteRowItem对象
        注意：对象内部存储ots2.metadata.TableInBatchWriteRowItem对象采用‘字典’的形式，Key是表
              的名字，因此如果插入同表名的对象，那么之前的对象将被覆盖。
        """
        if not isinstance(table_item, TableInBatchWriteRowItem):
            raise OTSClientError(
                "The input table_item should be an instance of TableInBatchWriteRowItem, not %s"%
                table_item.__class__.__name__
            )

        self.items[table_item.table_name] = table_item

class MultiTableInBatchWriteRowResult(object):

    def __init__(self, response):
        self.table_of_put = {}
        self.table_of_update = {}
        self.table_of_delete = {}

        for table in response:
            for type, rows in table.items():
                if len(rows) > 0:
                    row = rows[0]
                    if type == BatchWriteRowType.PUT:
                        self.table_of_put[row.table_name] = rows
                    elif type == BatchWriteRowType.UPDATE:
                        self.table_of_update[row.table_name] = rows
                    else:
                        self.table_of_delete[row.table_name] = rows

    def get_put(self):
        succ = []
        fail = []

        for rows in self.table_of_put.values():
            for row in rows:
                if row.is_ok:
                    succ.append(row)
                else:
                    fail.append(row)

        return succ, fail

    def get_put_by_table(self, table_name):
        return self.table_of_put[table_name]

    def get_failed_of_put(self):
        succ, fail = self.get_put()
        succ = None
        return fail

    def get_succeed_of_put(self):
        succ, fail = self.get_put()
        fail = None
        return succ

    def get_update(self):
        succ = []
        fail = []

        for rows in self.table_of_update.values():
            for row in rows:
                if row.is_ok:
                    succ.append(row)
                else:
                    fail.append(row)

        return succ, fail

    def get_update_by_table(self, table_name):
        return self.table_of_update[table_name]

    def get_failed_of_update(self):
        succ, fail = self.get_update()
        succ = None
        return fail

    def get_succeed_of_update(self):
        succ, fail = self.get_update()
        fail = None
        return succ

    def get_delete(self):
        succ = []
        fail = []

        for rows in self.table_of_delete.values():
            for row in rows:
                if row.is_ok:
                    succ.append(row)
                else:
                    fail.append(row)

        return succ, fail

    def get_delete_by_table(self, table_name):
        return self.table_of_delete[table_name]

    def get_failed_of_delete(self):
        succ, fail = self.get_delete()
        succ = None
        return fail

    def get_succeed_of_delete(self):
        succ, fail = self.get_delete()
        fail = None
        return succ

    def is_all_succeed(self):
        return len(self.get_failed_of_put()) == 0 and len(self.get_failed_of_update()) == 0 and len(self.get_failed_of_delete()) == 0

class BatchWriteRowResponseItem(object):

    def __init__(self, is_ok, error_code, error_message, table_name, consumed):
        self.is_ok = is_ok
        self.error_code = error_code
        self.error_message = error_message
        self.table_name = table_name
        self.consumed = consumed


class INF_MIN(object):
    # for get_range
    pass


class INF_MAX(object):
    # for get_range
    pass

