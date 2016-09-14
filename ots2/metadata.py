# -*- coding: utf8 -*-

from ots2.error import *
from ots2.utils import *

__all__ = [
    'INF_MIN',
    'INF_MAX',
    'TableMeta',
    'CapacityUnit',
    'ReservedThroughput',
    'ReservedThroughputDetails',
    'UpdateTableResponse',
    'DescribeTableResponse',
    'RowDataItem',
    'Condition',
    'PutRowItem',
    'UpdateRowItem',
    'DeleteRowItem',
    'BatchWriteRowResponseItem'
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


class UpdateTableResponse(object):

    def __init__(self, reserved_throughput_details):
        self.reserved_throughput_details = reserved_throughput_details


class DescribeTableResponse(object):

    def __init__(self, table_meta, reserved_throughput_details):
        self.table_meta = table_meta
        self.reserved_throughput_details = reserved_throughput_details


class RowDataItem(object):

    def __init__(self, is_ok, error_code, error_message, consumed, primary_key_columns, attribute_columns):
        # is_ok can be True or False
        # when is_ok is False,
        #     error_code & error_message are available
        # when is_ok is True,
        #     consumed & primary_key_columns & attribute_columns are available
        self.is_ok = is_ok
        self.error_code = error_code
        self.error_message = error_message
        self.consumed = consumed
        self.primary_key_columns = primary_key_columns
        self.attribute_columns = attribute_columns

class LogicalOperator(object):
    NOT = "NOT"
    AND = "AND"
    OR = "OR"

class ComparatorType(object):
    EQUAL = "EQUAL"
    NOT_EQUAL = "NOT_EQUAL"
    GREATER_THAN = "GREATER_THAN"
    GREATER_EQUAL = "GREATER_EQUAL"
    LESS_THAN = "LESS_THAN"
    LESS_EQUAL = "LESS_EQUAL"

class ColumnConditionType(object):
    COMPOSITE_CONDITION = "COMPOSITE_CONDITION"
    RELATION_CONDITION = "RELATION_CONDITION"

class ColumnCondition(object):
    
    def get_type(self):
        raise OTSClientError("ColumnCondition is abstract class, can not be an instance obj.")

class CompositeCondition(ColumnCondition):
    
    def __init__(self, combinator, sub_conditions = []):
        self.combinator = combinator
        self.sub_conditions = sub_conditions

    def get_type(self):
        return ColumnConditionType.COMPOSITE_CONDITION

    def set_combinator(self, combinator):
        if combinator not in GET_OBJ_DEFINE(ComparatorType):
            raise OTSClientError(
                "Expect input combinator in %s, but %s"%(str(GET_OBJ_DEFINE(LogicalOperator)), combinator)
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
   
    def __init__(self, column_name, comparator, column_value, pass_if_missing = True):
        self.column_name = column_name
        self.comparator = comparator
        self.column_value = column_value
        self.pass_if_missing = pass_if_missing

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
        if comparator not in GET_OBJ_DEFINE(ComparatorType):
            raise OTSClientError(
                "Expect input comparator in %s, but %s"%(str(GET_OBJ_DEFINE(ComparatorType)), comparator)
            )
        self.comparator = comparator

    def get_comparator(self):
        return self.comparator

class Condition(object):

    def __init__(self, row_existence_expectation, column_condition = None):
        self.row_existence_expectation = row_existence_expectation 
        self.column_condition = column_condition

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


class BatchWriteRowResponseItem(object):

    def __init__(self, is_ok, error_code, error_message, consumed):
        self.is_ok = is_ok
        self.error_code = error_code
        self.error_message = error_message
        self.consumed = consumed


class INF_MIN(object):
    # for get_range
    pass


class INF_MAX(object):
    # for get_range
    pass

