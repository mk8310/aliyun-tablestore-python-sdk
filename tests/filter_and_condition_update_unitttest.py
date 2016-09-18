# -*- coding: utf8 -*-

import unittest
from lib.ots2_api_test_base import OTS2APITestBase
import lib.restriction as restriction
from ots2 import *
from ots2.error import *
import time
import logging

class FilterAndConditionUpdateTest(OTS2APITestBase):
    TABLE_NAME = "test_filter_and_condition_update"

    """ConditionUpdate"""

    def test_put_row(self):
        """调用PutRow API, 构造不同的Condition"""
        table_name = FilterAndConditionUpdateTest.TABLE_NAME
        table_meta = TableMeta(table_name, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, reserved_throughput)

        time.sleep(5)
    
        ## RelationCondition
        ## EQUAL
         
        # 注入一行 index = 0
        primary_key = {'gid':0, 'uid':0}
        attribute_columns = {'index':0}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        # 注入一行，条件是index = 1时，期望写入失败
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 1, ComparatorType.EQUAL))
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是index = 0时，期望写入成功
        condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 0, ComparatorType.EQUAL))
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        # 注入一行，条件是addr = china时，因为该列不存在，期望写入失败
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("addr", "china", ComparatorType.EQUAL, False))
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 再次注入一行，条件是addr = china时，同时设置如果列不存在则不检查，期望写入失败
        condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("addr", "china", ComparatorType.EQUAL, True))
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## NOT_EQUAL

        # 注入一行，条件是index != 0时，期望写入失败
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 0, ComparatorType.NOT_EQUAL))
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是index != 1时，期望写入成功
        condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 1, ComparatorType.NOT_EQUAL))
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## GREATER_THAN

        # 注入一行，条件是index > 0时，期望写入失败
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 0, ComparatorType.GREATER_THAN))
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是index > -1时，期望写入成功
        condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", -1, ComparatorType.GREATER_THAN))
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## GREATER_EQUAL

        # 注入一行，条件是index >= 1时，期望写入失败
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 1, ComparatorType.GREATER_EQUAL))
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是index >= 0时，期望写入成功
        condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 0, ComparatorType.GREATER_EQUAL))
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## LESS_THAN

        # 注入一行，条件是index < 0时，期望写入失败
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 0, ComparatorType.LESS_THAN))
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是index < 1 时，期望写入成功
        condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 1, ComparatorType.LESS_THAN))
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## LESS_EQUAL

        # 注入一行，条件是index <= -1时，期望写入失败
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", -1, ComparatorType.LESS_EQUAL))
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是index <= 0 时，期望写入成功
        condition = Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 1, ComparatorType.LESS_EQUAL))
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## COMPOSITE_CONDITION
        attribute_columns = {'index':0, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## AND

        # 注入一行，条件是index == 0 & addr != china期望写入失败
        try:
            cond = CompositeCondition(LogicalOperator.AND)
            cond.add_sub_condition(RelationCondition("index", 0, ComparatorType.EQUAL))
            cond.add_sub_condition(RelationCondition("addr", 'china', ComparatorType.NOT_EQUAL))

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是index == 0 & addr == china 时，期望写入成功
        cond = CompositeCondition(LogicalOperator.AND)
        cond.add_sub_condition(RelationCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(RelationCondition("addr", 'china', ComparatorType.EQUAL))
        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## NOT

        # 注入一行，条件是!(index == 0 & addr == china)期望写入失败
        try:
            cond = CompositeCondition(LogicalOperator.NOT)
            sub_cond = CompositeCondition(LogicalOperator.AND)
            sub_cond.add_sub_condition(RelationCondition("index", 0, ComparatorType.EQUAL))
            sub_cond.add_sub_condition(RelationCondition("addr", 'china', ComparatorType.EQUAL))
            cond.add_sub_condition(sub_cond)

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是!(index != 0 & addr == china) 时，期望写入成功
        cond = CompositeCondition(LogicalOperator.NOT)
        
        sub_cond = CompositeCondition(LogicalOperator.AND)
        sub_cond.add_sub_condition(RelationCondition("index", 0, ComparatorType.NOT_EQUAL))
        sub_cond.add_sub_condition(RelationCondition("addr", 'china', ComparatorType.EQUAL))
        cond.add_sub_condition(sub_cond)

        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)

        ## OR

        # 注入一行，条件是index != 0 or addr != china期望写入失败
        try:
            cond = CompositeCondition(LogicalOperator.OR)
            cond.add_sub_condition(RelationCondition("index", 0, ComparatorType.NOT_EQUAL))
            cond.add_sub_condition(RelationCondition("addr", 'china', ComparatorType.NOT_EQUAL))

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.put_row(table_name, condition, primary_key, attribute_columns)
            self.assertTrue(False)
        except OTSServiceError, e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # 注入一行，条件是index == 0 or addr != china 时，期望写入成功
        cond = CompositeCondition(LogicalOperator.OR)
        cond.add_sub_condition(RelationCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(RelationCondition("addr", 'china', ComparatorType.NOT_EQUAL))
        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.put_row(table_name, condition, primary_key, attribute_columns)



if __name__ == '__main__':
    unittest.main()
