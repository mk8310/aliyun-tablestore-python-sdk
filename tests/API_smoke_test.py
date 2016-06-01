# -*- coding: utf8 -*-

from ots2_api_test_base import OTS2APITestBase
from ots2 import *
from ots2.error import *
import restriction
import time

class SmokeTestCase(OTS2APITestBase):

    """覆盖所有case lib的简单case"""
   ##################################################################
   #
    def test_assert_error(self):
        '''验证assert_error的可用性'''
        table_meta = TableMeta("table_**", [("PK", "STRING")])
        try:
            self.client_test.create_table(table_meta, ReservedThroughput(CapacityUnit(1, 1)))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "Invalid table name: 'table_**'.")

    def test_try_toconsumed(self):
        '''验证check_CU_by_consuming，try_to_consuming, get_row_size, wait_for_partition_load '''
        table_meta = TableMeta("table_test", [("PK", "STRING")])
        self.client_test.create_table(table_meta, ReservedThroughput(CapacityUnit(1, 1)))
        self.wait_for_partition_load('table_test')

        self.check_CU_by_consuming("table_test", {"PK": "1"}, {"PK": "not_in"}, CapacityUnit(1,1))

    def test_sum_CU_from_row(self):
        '''sum_CU_from_row, assert_consumed'''
        table_meta = TableMeta("table_test", [("PK", "STRING")])
        self.client_test.create_table(table_meta, ReservedThroughput(CapacityUnit(1, 1)))
        self.wait_for_partition_load('table_test')

        consumed = self.client_test.put_row("table_test", Condition("IGNORE"),{"PK": "1"}, {"COL":"new"})
        CU = self.sum_CU_from_row({"PK": "1"}, {"COL":"new"})
        self.assert_consumed(consumed, CapacityUnit(0, CU))

    def test_assert_UpdateCapacityResponse(self):
        '''BUG#268910 assert_UpdateTableCapacityResponse, assert_time, wait_for_capacity_unit_update'''
        table_meta = TableMeta("table_test", [("PK", "STRING")])
        self.client_test.create_table(table_meta, ReservedThroughput(CapacityUnit(1, 1)))
        self.wait_for_partition_load('table_test')

        time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
        expect_increase_time = int(time.time())
        res = self.client_test.update_table("table_test", ReservedThroughput(CapacityUnit(2, 3)))
	
        self.wait_for_capacity_unit_update('table_test')
        expect_res = UpdateTableResponse(ReservedThroughputDetails(CapacityUnit(2,3), expect_increase_time, None, 0))
        self.assert_UpdateTableResponse(res, expect_res)
        self.wait_for_CU_restore()

        self.check_CU_by_consuming("table_test", {"PK": "1"}, {"PK": "not_in"}, CapacityUnit(2,3))

    def test_assert_RowDataItem_equal(self):
        '''assert_RowDataItem_equal '''
        table_meta = TableMeta("table_test", [("PK", "STRING")])
        self.client_test.create_table(table_meta, ReservedThroughput(CapacityUnit(100, 100)))
        self.wait_for_partition_load('table_test')
        
        consumed = self.client_test.put_row("table_test", Condition("IGNORE"), {"PK": "1"}, {"COL":"new"})
        self.assert_consumed(consumed, CapacityUnit(0, 1))
 
        consumed, primary_keys, columns = self.client_test.get_row('table_test', {"PK": "1"})
        self.assert_consumed(consumed, CapacityUnit(1, 0))
        self.assert_equal(primary_keys, {"PK": "1"})
        self.assert_equal(columns, {"COL":"new"})

        batches = [("table_test", [{"PK": "1"}], [])]
        response = self.client_test.batch_get_row(batches)
        expect_row_data_item = RowDataItem(True, "", "", CapacityUnit(1, 0), {"PK": "1"}, {"COL":"new"})
        expect_response = [[expect_row_data_item]]
        self.assert_RowDataItem_equal(response, expect_response)
   
    def test_assert_BatchWriteRowResponseItem(self):
        '''BUG#268717 assert_BatchWriteRowResponseItem'''
        table_meta = TableMeta("table_test", [("PK", "STRING")])
        self.client_test.create_table(table_meta, ReservedThroughput(CapacityUnit(100, 100)))
        self.wait_for_partition_load('table_test')

        put_row_item = {"table_name": "table_test", "put": [PutRowItem(Condition("IGNORE"), {"PK": "xxxxx"}, {'COL': 'x'})]}
        write_batches = [put_row_item]
        response = self.client_test.batch_write_row(write_batches)
        expect_write_data_item = {"put": [BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, 1))]}
        expect_response = [expect_write_data_item]
        self.assert_BatchWriteRowResponseItem(response, expect_response)

    def test_assert_DescribeTableResponse(self):
        '''assert_DescribeTableResponse, assert_TableMeta'''
        table_meta = TableMeta("table_test", [("PK", "STRING")])
        self.client_test.create_table(table_meta, ReservedThroughput(CapacityUnit(100, 200)))
        self.wait_for_partition_load('table_test')

        response = self.client_test.describe_table("table_test")
        self.assert_DescribeTableResponse(response, CapacityUnit(100, 200), table_meta)



