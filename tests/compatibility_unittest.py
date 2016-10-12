# -*- coding: utf8 -*-

import unittest
from lib.ots2_api_test_base import OTS2APITestBase
import lib.restriction as restriction
from ots2 import *
from ots2.error import *
import time
import logging

class FilterAndConditionUpdateTest(OTS2APITestBase):
    TABLE_NAME = "test_compatibility"

    def test_batch_write_row(self): 
        """调用BatchWriteRow API, 测试旧的方式调用仍然有效"""
        table_meta = TableMeta('myTable0', [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, reserved_throughput)

        table_meta = TableMeta('myTable1', [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, reserved_throughput)

        time.sleep(5)

        primary_key = {'gid':0, 'uid':0}
        attribute_columns = {'index':0, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable0', condition, primary_key, attribute_columns)

        primary_key = {'gid':0, 'uid':1}
        attribute_columns = {'index':1, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable0', condition, primary_key, attribute_columns)

        primary_key = {'gid':0, 'uid':2}
        attribute_columns = {'index':2, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable0', condition, primary_key, attribute_columns)

        primary_key = {'gid':0, 'uid':3}
        attribute_columns = {'index':3, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable1', condition, primary_key, attribute_columns)

        primary_key = {'gid':0, 'uid':4}
        attribute_columns = {'index':4, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable1', condition, primary_key, attribute_columns)

        primary_key = {'gid':0, 'uid':5}
        attribute_columns = {'index':5, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable1', condition, primary_key, attribute_columns)


        # put
        put_row_items = []
        put_row_items.append(PutRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 0, ComparatorType.EQUAL)),  
            {'gid':0, 'uid':0}, 
            {'index':6, 'addr':'china'}))

        put_row_items.append(PutRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 1, ComparatorType.EQUAL)),  
            {'gid':0, 'uid':1}, 
            {'index':7, 'addr':'china'}))

        put_row_items.append(PutRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 2, ComparatorType.EQUAL)),  
            {'gid':0, 'uid':2}, 
            {'index':8, 'addr':'china'}))

        batch_list = []
        batch_list.append({
            'table_name':'myTable0', 
            'put':put_row_items})
        batch_list.append({
            'table_name':'myTable1', 
            'put':put_row_items})

        result = self.client_test.batch_write_row(batch_list)

        put0 = result[0]['put']
        put1 = result[1]['put']

        self.assertEqual(3, len(put0))
        self.assertEqual(3, len(put1))

        for i in put0:
            self.assertTrue(i.is_ok)
            self.assertEqual(1, i.consumed.write)
            self.assertEqual(1, i.consumed.read)

        for i in put1:
            self.assertTrue(i.is_ok)
            self.assertEqual(1, i.consumed.write)
            self.assertEqual(1, i.consumed.read)

        # update
        update_row_items = []
        update_row_items.append(UpdateRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 0, ComparatorType.EQUAL)),  
            {'gid':0, 'uid':0}, 
            {
                'put': {'index':9, 'addr':'china'}
            }))

        update_row_items.append(UpdateRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 1, ComparatorType.EQUAL)),  
            {'gid':0, 'uid':1}, 
            {
                'put': {'index':10, 'addr':'china'}
            }))


        update_row_items.append(UpdateRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 2, ComparatorType.EQUAL)),  
            {'gid':0, 'uid':2}, 
            {
                'put': {'index':11, 'addr':'china'}
            }))


        batch_list = []
        batch_list.append({'table_name':'myTable0', 'update':update_row_items})
        batch_list.append({'table_name':'myTable1', 'update':update_row_items})

        result = self.client_test.batch_write_row(batch_list)

        update0 = result[0]['update']
        update1 = result[1]['update']

        self.assertEqual(3, len(update0))
        self.assertEqual(3, len(update1))

        for i in update0:
            self.assertFalse(i.is_ok)
            self.assertEqual('OTSConditionCheckFail', i.error_code)

        for i in update1:
            self.assertFalse(i.is_ok)
            self.assertEqual('OTSConditionCheckFail', i.error_code)

        # delete
        delete_row_items = []
        delete_row_items.append(DeleteRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 3, ComparatorType.EQUAL, False)),  
            {'gid':0, 'uid':0}))

        delete_row_items.append(DeleteRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 4, ComparatorType.EQUAL, False)),  
            {'gid':0, 'uid':1}))

        delete_row_items.append(DeleteRowItem(
            Condition(RowExistenceExpectation.IGNORE, RelationCondition("index", 5, ComparatorType.EQUAL, False)),  
            {'gid':0, 'uid':2}))

        batch_list = []
        batch_list.append({'table_name':'myTable0', 'delete': delete_row_items})
        batch_list.append({'table_name':'myTable1', 'delete': delete_row_items})

        result = self.client_test.batch_write_row(batch_list)

        delete0 = result[0]['delete']
        delete1 = result[1]['delete']

        self.assertEqual(3, len(delete0))
        self.assertEqual(3, len(delete1))

        for i in delete0:
            self.assertFalse(i.is_ok)
            self.assertEqual("OTSConditionCheckFail", i.error_code)
 
        for i in delete1:
            self.assertFalse(i.is_ok)
            self.assertEqual("OTSConditionCheckFail", i.error_code)
 
    def test_batch_get_row(self):
        """调用BatchGetRow API, 测试以前的方式仍然有效"""
        table_meta = TableMeta('myTable0', [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, reserved_throughput)

        table_meta = TableMeta('myTable1', [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, reserved_throughput)

        time.sleep(5)
 
        primary_key = {'gid':0, 'uid':0}
        attribute_columns = {'index':0, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable0', condition, primary_key, attribute_columns)

        primary_key = {'gid':0, 'uid':1}
        attribute_columns = {'index':1, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable0', condition, primary_key, attribute_columns)

        primary_key = {'gid':0, 'uid':2}
        attribute_columns = {'index':2, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable0', condition, primary_key, attribute_columns)

        primary_key = {'gid':0, 'uid':0}
        attribute_columns = {'index':0, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable1', condition, primary_key, attribute_columns)

        primary_key = {'gid':1, 'uid':0}
        attribute_columns = {'index':1, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable1', condition, primary_key, attribute_columns)

        primary_key = {'gid':2, 'uid':0}
        attribute_columns = {'index':2, 'addr':'china'}
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row('myTable1', condition, primary_key, attribute_columns)


        # 读取一行数据，(index != 0 & addr == china), 期望读取失败
        column_to_get = ['gid', 'uid', 'index']
        
        batch_list = []

        primary_keys = []
        primary_keys.append({'gid':0, 'uid':0})
        primary_keys.append({'gid':0, 'uid':1})
        primary_keys.append({'gid':0, 'uid':2})
        batch_list.append(('myTable0', primary_keys, column_to_get))

        primary_keys = []
        primary_keys.append({'gid':0, 'uid':0})
        primary_keys.append({'gid':1, 'uid':0})
        primary_keys.append({'gid':2, 'uid':0})
        batch_list.append(('myTable1', primary_keys, column_to_get))

        result = self.client_test.batch_get_row(batch_list)

        self.assertEqual(2, len(result))

        table0 = result[0]
        table1 = result[1]

        self.assertEqual(3, len(table0))
        self.assertEqual(3, len(table1))

        # myTable0
        # row 0
        self.assertEqual({'gid':0, 'uid':0}, table0[0].primary_key_columns)
        self.assertEqual({'index':0}, table0[0].attribute_columns)

        # row 1
        self.assertEqual({'gid':0, 'uid':1}, table0[1].primary_key_columns)
        self.assertEqual({'index': 1}, table0[1].attribute_columns)

        # row 2
        self.assertEqual({'gid':0, 'uid':2}, table0[2].primary_key_columns)
        self.assertEqual({'index': 2}, table0[2].attribute_columns)

        # myTable1
        # row 0
        self.assertEqual({'gid':0, 'uid':0}, table1[0].primary_key_columns)
        self.assertEqual({'index':0}, table0[0].attribute_columns)

        # row 1
        self.assertEqual({'gid':1, 'uid':0}, table1[1].primary_key_columns)
        self.assertEqual({'index': 1}, table1[1].attribute_columns)

        # row 2
        self.assertEqual({'gid':2, 'uid':0}, table1[2].primary_key_columns)
        self.assertEqual({'index': 2}, table1[2].attribute_columns)

        ## RELATION_CONDITION
        column_to_get = ['gid', 'uid', 'index']
        
        batch_list = []

        primary_keys = []
        primary_keys.append({'gid':0, 'uid':0})
        primary_keys.append({'gid':0, 'uid':1})
        primary_keys.append({'gid':0, 'uid':2})
        batch_list.append(('myTable0', primary_keys, column_to_get))

        primary_keys = []
        primary_keys.append({'gid':0, 'uid':0})
        primary_keys.append({'gid':1, 'uid':0})
        primary_keys.append({'gid':2, 'uid':0})
        batch_list.append(('myTable1', primary_keys, column_to_get))

        result = self.client_test.batch_get_row(batch_list)

        self.assertEqual(2, len(result))

        table0 = result[0]
        table1 = result[1]

        self.assertEqual(3, len(table0))
        self.assertEqual(3, len(table1))

        # myTable0
        # row 0
        self.assertEqual({'gid':0, 'uid':0}, table0[0].primary_key_columns)
        self.assertEqual({'index': 0}, table0[0].attribute_columns)

        # row 1
        self.assertEqual({'gid':0, 'uid':1}, table0[1].primary_key_columns)
        self.assertEqual({'index': 1}, table0[1].attribute_columns)

        # row 2
        self.assertEqual({'gid':0, 'uid':2}, table0[2].primary_key_columns)
        self.assertEqual({'index': 2}, table0[2].attribute_columns)

        # myTable1
        # row 0
        self.assertEqual({'gid':0, 'uid':0}, table1[0].primary_key_columns)
        self.assertEqual({'index': 0}, table0[0].attribute_columns)

        # row 1
        self.assertEqual({'gid':1, 'uid':0}, table1[1].primary_key_columns)
        self.assertEqual({'index': 1}, table1[1].attribute_columns)

        # row 2
        self.assertEqual({'gid':2, 'uid':0}, table1[2].primary_key_columns)
        self.assertEqual({'index': 2}, table1[2].attribute_columns)

if __name__ == '__main__':
    unittest.main()
