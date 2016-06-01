# -*- coding:utf-8 -*-

import restriction
import ots2_api_test_config
from ots2_api_test_base import OTS2APITestBase
from ots2.error import *

from ots2 import *
import string
import copy
import time
import math
import atest.log

class RestrictionTest(OTS2APITestBase):

    """限制项测试"""
    ####################################################################################################
    # 杨恋请从这里开始
    def _check_name_length_expect_exception(
            self, client_instance, table_name, row1_pk, 
            row2_pk, start_pk, end_pk, message="some message", error_code="OTSParameterInvalid", column_name='col1'):
        try:
            consumed = client_instance.put_row(table_name, Condition("IGNORE"), row1_pk, {column_name:'1'})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, error_code, message)

        try:
            consumed, primary_keys, columns = client_instance.get_row(table_name, row1_pk, [column_name])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, error_code, message)

        try:
            consumed = client_instance.update_row(table_name, Condition("IGNORE"), row1_pk, {'put':{column_name:'22'}})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, error_code, message)

        try:
            get_response = client_instance.batch_get_row([(table_name, [row1_pk], [column_name])])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, error_code, message)

        put_row_item = PutRowItem(Condition("IGNORE"), row2_pk, {column_name:'100'})
        update_row_item = UpdateRowItem(Condition("IGNORE"), row2_pk, {'put':{column_name:'200'}})
        delete_row_item = DeleteRowItem(Condition("IGNORE"), row2_pk)
        if column_name=='col1':
            batch_list = [{'put':[put_row_item]}, {'update':[update_row_item]}, {'delete':[delete_row_item]}]
        else:
            batch_list = [{'put':[put_row_item]}, {'update':[update_row_item]}]
        for i in range(len(batch_list)):
            write_row = batch_list[i]
            write_row['table_name'] = table_name
            try:
                write_response = client_instance.batch_write_row([write_row])
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, error_code, message)
 
        try:
            consumed, next_start_primary_keys, rows = client_instance.get_range(table_name, 'FORWARD', start_pk, end_pk, [column_name])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, error_code, message)


    def _check_name_length_expect_none_exception(
            self, client_instance, table_name, row1_pk, 
            row2_pk, start_pk, end_pk, column_name='col1'):
        consumed = client_instance.put_row(table_name, Condition("IGNORE"), row1_pk, {column_name:'1'})
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(row1_pk, {column_name:'1'})))
        self.wait_for_partition_load(table_name)

        consumed, primary_keys, columns = client_instance.get_row(table_name, row1_pk, [column_name])
        self.assert_consumed(consumed, CapacityUnit(self.sum_CU_from_row(row1_pk, {column_name:'1'}), 0))
        self.assert_equal(primary_keys, {})
        self.assert_equal(columns, {column_name:'1'})

        consumed = client_instance.update_row(table_name, Condition("IGNORE"), row1_pk, {'put':{column_name:'2'}})
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(row1_pk, {column_name:'2'})))
        self.wait_for_partition_load(table_name)

        column = {column_name:'2'}
        consumed_expect = CapacityUnit(self.sum_CU_from_row(row1_pk, column), 0)
        #get_row_item = RowDataItem(True, "", "", consumed_expect, row1_pk, column)
        get_row_item = RowDataItem(True, "", "", consumed_expect, {}, column)
        get_response = client_instance.batch_get_row([(table_name, [row1_pk], [column_name])])
        self.assert_RowDataItem_equal(get_response, [[get_row_item]])

        put_row_item = PutRowItem(Condition('EXPECT_NOT_EXIST'), row2_pk, {column_name:'100'})
        update_row_item = UpdateRowItem(Condition('EXPECT_EXIST'), row2_pk, {'put':{column_name:'200'}})
        delete_row_item = DeleteRowItem(Condition("IGNORE"), row2_pk)
        batch_list = [{'put':[put_row_item]}, {'update':[update_row_item]}, {'delete':[delete_row_item]}]
        expect_write_cu = self.sum_CU_from_row(row2_pk, {column_name:'200'})
        response_list = [
            {'put':[BatchWriteRowResponseItem(True, '', '', CapacityUnit(1, expect_write_cu))]}, 
            {'update':[BatchWriteRowResponseItem(True, '', '', CapacityUnit(1, expect_write_cu))]}, 
            {'delete':[BatchWriteRowResponseItem(True, '', '', CapacityUnit(0, self.sum_CU_from_row(row2_pk, {})))]}
        ]
        for i in range(len(batch_list)):
            write_row = batch_list[i]
            write_row['table_name'] = table_name
            write_response = client_instance.batch_write_row([write_row])
            self.assert_BatchWriteRowResponseItem(write_response, [response_list[i]])
            self.wait_for_partition_load(table_name)
 
        consumed, next_start_primary_keys, rows = client_instance.get_range(table_name, 'FORWARD', start_pk, end_pk, [column_name])
        consumed_expect = CapacityUnit(self.sum_CU_from_row(row1_pk, column), 0)
        self.assert_consumed(consumed, consumed_expect)
        self.assert_equal(next_start_primary_keys, None)
        self.assert_equal(rows, [({}, column)])

        consumed = client_instance.delete_row(table_name, Condition("IGNORE"), row1_pk)
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(row1_pk, column)))

    def test_instance_name_length_exceeded(self):
        """BUG#268704 对于每个API, instance name长度为 max_length + 1，期望返回ErrorCode: OTSParameterInvalid"""
        instance_name = 'X' * (restriction.MaxInstanceNameLength + 1)

        client = OTSClient(
            ots2_api_test_config.endpoint,
            ots2_api_test_config.access_id,
            ots2_api_test_config.access_key,
            instance_name,
        )

        table_name = 'MMMM'
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))

        row1_pk = {'PK0':'A', 'PK1':'10'}
        row2_pk = {'PK0':'A', 'PK1':'20'}
        start_pk = {'PK0':'A', 'PK1':'1'}
        end_pk = {'PK0':'B', 'PK1':'100'}
        
        error_message = "Invalid instance name: '%s'." % instance_name

        try:
            client.list_table()
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            client.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            update_table_response = client.update_table(table_name, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            discribe_table_response = client.describe_table(table_name)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        self._check_name_length_expect_exception(client, table_name, row1_pk, row2_pk, 
                                                start_pk, end_pk, error_message)

        try:
            consumed = client.delete_row(table_name, Condition("IGNORE"), row1_pk)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            client.delete_table(table_name)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)
    
    def test_instance_name_max_length(self):
        """BUG#269084 对于每个API, instance name长度为 max_length ，期望正常"""
        def _valid_instance_op(client):
            table_name = "table_test"
            table_meta = TableMeta(table_name, [("PK", "STRING")])
            reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
            #create_table
            expect_increase_time = int(time.time())
            client.create_table(table_meta, reserved_throughput)
            self.wait_for_partition_load('table_test')

            #update_table
            time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
            update_table_response = client.update_table(table_name, ReservedThroughput(CapacityUnit(1000, 2000)))
            expect_decrease_time = int(time.time())
            expect_res = UpdateTableResponse(ReservedThroughputDetails(CapacityUnit(1000, 2000), expect_increase_time, expect_decrease_time, 1))
            self.assert_UpdateTableResponse(update_table_response, expect_res)
            self.wait_for_capacity_unit_update('table_test')
            #describe_table
            response = client.describe_table(table_name)
            self.assert_DescribeTableResponse(response, CapacityUnit(1000, 2000), table_meta)
            #get_row
            consumed, primary_keys, columns = client.get_row(table_name, {"PK": "x"})
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(primary_keys, {})
            self.assert_equal(columns, {})
            #batch_get_row
            batches = [(table_name, [{"PK": "x"}], [])]
            response = client.batch_get_row(batches)
            expect_row_data_item = RowDataItem(True, "", "", CapacityUnit(1, 0), {}, {})
            expect_response = [[expect_row_data_item]]
            self.assert_RowDataItem_equal(response, expect_response)
            #get_range
            consumed, next_start_primary_keys, rows = client.get_range(table_name, 'FORWARD', {"PK": INF_MIN}, {"PK": INF_MAX})
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(next_start_primary_keys, None)
            self.assert_equal(rows, [])
            #put_row
            consumed = client.put_row(table_name, Condition("IGNORE"), {"PK": "x"}, {'COL': 'x'})
            self.assert_consumed(consumed, CapacityUnit(0, 1))
            #update_row
            consumed = client.update_row(table_name, Condition("IGNORE"), {"PK": "x"}, {'put':{"COL": "x1"}})
            self.assert_consumed(consumed, CapacityUnit(0, 1))
            #delete_row
            consumed = client.delete_row(table_name, Condition("IGNORE"), {"PK": "x"}) 
            self.assert_consumed(consumed, CapacityUnit(0, 1))
            #batch_write_row
            put_row_item = PutRowItem(Condition("IGNORE"), {"PK": "x"}, {'COL': 'x'})
            update_row_item = UpdateRowItem(Condition("IGNORE"), {"PK": "x"}, {'put':{'COL': 'x1'}})
            delete_row_item = DeleteRowItem(Condition("IGNORE"), {"PK": "x"})
            batches_list = [{'put':[put_row_item]}, {'update':[update_row_item]}, {'delete':[delete_row_item]}]
            expect_write_data_item = BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, 1))
            response_list = [{'put':[expect_write_data_item]}, {'update':[expect_write_data_item]}, {'delete':[expect_write_data_item]}]
            for i in range(len(batches_list)):
                write_batches = batches_list[i]
                write_batches['table_name'] = table_name
                response = client.batch_write_row([write_batches])
                expect_response = [response_list[i]]
                self.assert_BatchWriteRowResponseItem(response, expect_response)
            #delete_table
            client.delete_table(table_name)
            #list_table
            table_list = client.list_table()
            self.assert_equal(table_list, ())

        instance_name = "t" * restriction.MaxInstanceNameLength 
        self._create_instance([instance_name]) 
        ots_client = OTSClient(
                ots2_api_test_config.endpoint,
                ots2_api_test_config.access_id,
                ots2_api_test_config.access_key,
                instance_name,
                logger_name=atest.log.root.name
                )
        
        _valid_instance_op(ots_client)
        pass
    def test_table_name_length_exceeded(self):
        """对于每个参数包含table_name的API，table_name长度为 max_length + 1，期望返回ErrorCode: OTSParameterInvalid"""
        table_name = 'M' * (restriction.MaxTableNameLength + 1)
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))

        row1_pk = {'PK0':'A', 'PK1':'10'}
        row2_pk = {'PK0':'A', 'PK1':'20'}
        start_pk = {'PK0':'A', 'PK1':'1'}
        end_pk = {'PK0':'B', 'PK1':'100'}

        expect_message = "Invalid table name: '%s'." % table_name
        try:
            self.client_test.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", expect_message) 

        try:
            update_table_response = self.client_test.update_table(table_name, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", expect_message)

        try:
            discribe_table_response = self.client_test.describe_table(table_name)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", expect_message)

        self._check_name_length_expect_exception(self.client_test, table_name, row1_pk, 
                                                     row2_pk, start_pk, end_pk, expect_message)

        try:
            consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), row1_pk)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", expect_message)

        try:
            self.client_test.delete_table(table_name)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", expect_message)

         
    def test_table_name_with_max_length(self):
        """BUG#268507 BUG#268717 BUG#268918 BUG#268743 对于每个参数包含table_name的API，table_name长度为 max_length ，期望正常"""
        table_name = 'M' * restriction.MaxTableNameLength
        table_meta = TableMeta(table_name, [('PK0' ,'STRING'), ('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))

        row1_pk = {'PK0':'B', 'PK1':'10'}
        row2_pk = {'PK0':'B', 'PK1':'20'}
        start_pk = {'PK0':'A', 'PK1':'1'}
        end_pk = {'PK0':'C', 'PK1':'100'}

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load(table_name)

        new_reserved_throughput = ReservedThroughput(CapacityUnit(20,20))
        time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
        update_table_response = self.client_test.update_table(table_name, new_reserved_throughput)

        discribe_table_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(discribe_table_response, new_reserved_throughput.capacity_unit, table_meta)

        self._check_name_length_expect_none_exception(self.client_test, table_name, row1_pk, 
                                                     row2_pk, start_pk, end_pk)

        self.client_test.delete_table(table_name)

    def _column_name_length_check(self, table_name_common, expect_column_name, expect_exception):
        pk_list = [('PK0', 'STRING'), ('PK1', 'STRING'), ('PK2', 'STRING'), ('PK3', 'STRING')]
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
        error_message = "Invalid column name: '%s'." % expect_column_name
        #PK
        for i in range(4):
            row1_pk = {}
            row2_pk = {}
            start_pk = {}
            end_pk = {}
            table_name = table_name_common + str(i)
            pk_list_copy = copy.copy(pk_list)
            pk_list_copy[i] = (expect_column_name, 'STRING')    
        #row1_pk = {'PK0':'A', 'PK1':'A', 'PK2':'A', 'PK3':'A'}
        #row2_pk = {'PK0':'A', 'PK1':'B', 'PK2':'B', 'PK3':'B'}
        #start_pk = {'PK0':'A', 'PK1':'A', 'PK2':'A', 'PK3':'A'}
        #end_pk = {'PK0':'B', 'PK1':'B', 'PK2':'B', 'PK3':'B'}
            for pk_schema in pk_list_copy:
                (pk_name, pk_type) = pk_schema
                row1_pk[pk_name] = 'A'
                row2_pk[pk_name] = 'B'
                start_pk[pk_name] = 'A'
                end_pk[pk_name] = 'B'
                
            table_meta = TableMeta(table_name, pk_list_copy)
            try:
                self.client_test.create_table(table_meta, reserved_throughput)
                if expect_exception == True:
                    self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", error_message) 
            self.wait_for_partition_load(table_name)

            if expect_exception == True:
                self._check_name_length_expect_exception(self.client_test, table_name, row1_pk, 
                                                    row2_pk, start_pk, end_pk, error_message)
                try:
                    consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), row1_pk)
                    self.assert_false()
                except OTSServiceError as e:
                    self.assert_error(e, 400, "OTSParameterInvalid", error_message)
            else:
                self._check_name_length_expect_none_exception(self.client_test, table_name, row1_pk,
                                                    row2_pk, start_pk, end_pk)
        #columns
        row1_pk = {}
        row2_pk = {}
        start_pk = {}
        end_pk = {}
        table_name = table_name_common
        for pk_schema in pk_list:
            (pk_name, pk_type) = pk_schema
            row1_pk[pk_name] = 'A'
            row2_pk[pk_name] = 'B'
            start_pk[pk_name] = 'A'
            end_pk[pk_name] = 'B'

        table_meta = TableMeta(table_name, pk_list)
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load(table_name)

        if expect_exception == True:
            self._check_name_length_expect_exception(self.client_test, table_name, row1_pk,
                                                row2_pk, start_pk, end_pk, error_message, "OTSParameterInvalid", expect_column_name)
        else:
            self._check_name_length_expect_none_exception(self.client_test, table_name, row1_pk,
                                                row2_pk, start_pk, end_pk, expect_column_name)

    def test_column_name_length_exceeded(self):
        """对于每个参数包含column name的API，column name长度为 max_length + 1，期望返回ErrorCode: OTSParameterInvalid"""
        table_name_common = 'test_table_for_exceeded_length'
        column_name_exceed = 'M' * (restriction.MaxColumnNameLength + 1)
        self._column_name_length_check(table_name_common, column_name_exceed, True)

    def test_column_name_with_max_length(self):
        """BUG#268918 BUG#268743 对于每个参数包含column name的API，column name长度为 max_length，期望正常"""
        table_name_common = 'test_table_for_max_length'
        column_name = 'M' * restriction.MaxColumnNameLength
        self._column_name_length_check(table_name_common, column_name, False)

    def test_column_name_empty(self):
        """对于每个参数包含column name的API，column name长度为 0，期望返回ErrorCode: OTSParameterInvalid"""
        table_name_common = 'test_table_for_empty_column_name'
        column_name = ''
        self._column_name_length_check(table_name_common, column_name, True)

    def test_table_num_limit_of_instance(self):
        """一个instance有max个table，创建一个表期望失败，删除一个表后，再创建一个表期望成功"""
        count = 1
        while count <= restriction.MaxTableCountForInstance:
            table_name = 'table_test_max_num' + str(count)
            table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
            reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
            self.client_test.create_table(table_meta, reserved_throughput)
            count = count + 1
        table_name = 'table_test_max_num' + str(count)
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
        try:
            self.client_test.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSQuotaExhausted", "Number of tables exceeded the quota.") #TODO error_code可能不是这个

        count = count - 1
        table_name = 'table_test_max_num' + str(count)
        self.client_test.delete_table(table_name)

        table_name = 'table_test_max_num' + str(count)
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(10,10))
        self.client_test.create_table(table_meta, reserved_throughput)

    def test_primary_keys_number_exceeded(self):
        """对于每个参数包含primary_keys的API，primary_keys的个数为max + 1，期望返回ErrorCode: OTSParameterInvalid"""
        table_name = 'test_table_for_exceeded_pk_num'
        pk_list, pk_dict = self.get_primary_keys(restriction.MaxPKColumnNum + 1, 'STRING', 'PK', '1')
        table_meta = TableMeta(table_name, pk_list)
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
        error_message = "The number of primary key columns must be in range: [1, %d]." % restriction.MaxPKColumnNum

        try:
            self.client_test.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message) 

        try:
            consumed, primary_keys, columns = self.client_test.get_row(table_name, pk_dict)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            consumed = self.client_test.put_row(table_name, Condition("IGNORE"), pk_dict, {'col1':'1'})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        pk_dict['PK' + str(restriction.MaxPKColumnNum)] = '10'
        try:
            consumed = self.client_test.update_row(table_name, Condition("IGNORE"), pk_dict, {'put':{'col1':'1'}})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), pk_dict)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        pk_dict['PK' + str(restriction.MaxPKColumnNum)] = '1'
        try:
            get_response = self.client_test.batch_get_row([(table_name, [pk_dict], [])])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        put_row_item = PutRowItem(Condition("IGNORE"), pk_dict, {'col1':'100'})
        update_row_item = UpdateRowItem(Condition("IGNORE"), pk_dict, {'put':{'col1':'200'}})
        delete_row_item = DeleteRowItem(Condition("IGNORE"), pk_dict)
        batch_list = [{'put':[put_row_item]}, {'update':[update_row_item]}, {'delete':[delete_row_item]}]
        for i in range(len(batch_list)):
            write_row = batch_list[i]
            write_row['table_name'] = table_name
            try:
                write_response = self.client_test.batch_write_row([write_row])
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        pk_dict2 = copy.copy(pk_dict)
        pk_dict2['PK1'] = '2'
        try:
            consumed, next_start_primary_keys, rows = self.client_test.get_range(table_name, 'FORWARD', pk_dict, pk_dict2)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

    def test_primary_keys_number_is_max(self):
        """BUG#268717 对于每个参数包含primary_keys的API，primary_keys的个数为max，期望正常"""
        table_name = 'test_table_for_max_pk_num'
        pk_list, pk_dict = self.get_primary_keys(restriction.MaxPKColumnNum, 'STRING', 'PK', '1')
        table_meta = TableMeta(table_name, pk_list)
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('test_table_for_max_pk_num')

        consumed = self.client_test.put_row(table_name, Condition("IGNORE"), pk_dict, {'col1':'1'})
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(pk_dict, {'col1':'1'})))

        consumed, primary_keys, columns = self.client_test.get_row(table_name, pk_dict)
        self.assert_consumed(consumed, CapacityUnit(self.sum_CU_from_row(pk_dict, {'col1':'1'}), 0))
        self.assert_equal(primary_keys, pk_dict)
        self.assert_equal(columns, {'col1':'1'})

        pk_dict['PK' + str(restriction.MaxPKColumnNum - 1)] = '10'
        consumed = self.client_test.update_row(table_name, Condition("IGNORE"), pk_dict, {'put':{'col1':'1'}})
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(pk_dict, {'col1':'1'})))

        consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), pk_dict)
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(pk_dict, {'col1':'1'})))

        pk_dict['PK' + str(restriction.MaxPKColumnNum - 1)] = '1'
        column = {'col1':'1'}
        consumed_expect = CapacityUnit(self.sum_CU_from_row(pk_dict, column), 0)
        get_row_item = RowDataItem(True, "", "", consumed_expect, pk_dict, column)
        get_response = self.client_test.batch_get_row([(table_name, [pk_dict], [])])
        self.assert_RowDataItem_equal(get_response, [[get_row_item]])

        pk_dict['PK' + str(restriction.MaxPKColumnNum - 1)] = '100'
        put_row_item = PutRowItem(Condition("IGNORE"), pk_dict, {'col1':'100'})
        update_row_item = UpdateRowItem(Condition('EXPECT_EXIST'), pk_dict, {'put':{'col1':'200'}})
        delete_row_item = DeleteRowItem(Condition("IGNORE"), pk_dict)
        batch_list = [{'put':[put_row_item]}, {'update':[update_row_item]}, {'delete':[delete_row_item]}]
        expect_write_cu = self.sum_CU_from_row(pk_dict, {'col1':'200'})
        response_list = [
            {'put':[BatchWriteRowResponseItem(True, '', '', CapacityUnit(0, expect_write_cu))]}, 
            {'update':[BatchWriteRowResponseItem(True, '', '', CapacityUnit(1, expect_write_cu))]}, 
            {'delete':[BatchWriteRowResponseItem(True, '', '', CapacityUnit(0, self.sum_CU_from_row(pk_dict, {})))]}
        ]
        for i in range(len(batch_list)):
            write_row = batch_list[i]
            write_row['table_name'] = table_name
            write_response = self.client_test.batch_write_row([write_row])
            self.assert_BatchWriteRowResponseItem(write_response, [response_list[i]])

        pk_dict['PK' + str(restriction.MaxPKColumnNum - 1)] = '1'
        pk_dict2 = copy.copy(pk_dict)
        pk_dict2['PK1'] = '2'
        consumed, next_start_primary_keys, rows = self.client_test.get_range(table_name, 'FORWARD', pk_dict, pk_dict2)
        self.assert_consumed(consumed, CapacityUnit(self.sum_CU_from_row(pk_dict, column), 0))
        self.assert_equal(next_start_primary_keys, None)
        self.assert_equal(rows, [(pk_dict, {'col1':'1'})])

    def test_primary_keys_number_zero(self):
        """BUG#268868 对于每个参数包含primary_keys的API，primary_keys的个数为0，期望返回ErrorCode: OTSParameterInvalid"""
        pk_dict = {}
        table_name = 'test_table_for_none_pk_num'
        count = 1
        table_meta = TableMeta(table_name, [])
        reserved_throughput = ReservedThroughput(CapacityUnit(10,10))
        error_message = "The number of primary key columns must be in range: [1, %d]." % restriction.MaxPKColumnNum

        try:
            self.client_test.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            consumed, primary_keys, columns = self.client_test.get_row(table_name, pk_dict)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            consumed = self.client_test.put_row(table_name, Condition("IGNORE"), pk_dict, {'col1':'1'})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            consumed = self.client_test.update_row(table_name, Condition("IGNORE"), pk_dict, {'put':{'col1':'1'}})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), pk_dict)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        try:
            get_response = self.client_test.batch_get_row([(table_name, [pk_dict], [])])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

        put_row_item = PutRowItem(Condition("IGNORE"), pk_dict, {'col1':'100'})
        update_row_item = UpdateRowItem(Condition("IGNORE"), pk_dict, {'put':{'col1':'200'}})
        delete_row_item = DeleteRowItem(Condition("IGNORE"), pk_dict)
        batch_list = [{'put':[put_row_item]}, {'update':[update_row_item]}, {'delete':[delete_row_item]}]
        for i in range(len(batch_list)):
            write_row = batch_list[i]
            write_row['table_name'] = table_name
            try:
                write_response = self.client_test.batch_write_row([write_row])
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", error_message)
       
        pk_dict2 = {"pk_test":'1'}
        try:
            consumed, next_start_primary_keys, rows = self.client_test.get_range(table_name, 'FORWARD', pk_dict, pk_dict)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", error_message)

    ####################################################################################################
    # 淑婷请从这里开始
    def test_pk_binary_value_length_exceeded(self):
        """对于每个参数包含primary_keys的API，PK的binary value长度为max + 1，期望返回ErrorCode: OTSParameterInvalid"""
        
        #create test table
        table_name = "table_test"
        pk_schema, valid_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "BINARY", pk_name="PK", pk_value=bytearray(2))

        table_meta = TableMeta(table_name, pk_schema)
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')
        
        invalid_pk = bytearray("x" * (restriction.MaxPKStringValueLength + 1))
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = invalid_pk
            #get row
            try:
                self.client_test.get_row(table_name, primary_keys, [])
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #put_row
            try:
                self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, {'COL': 'x'})
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #update_row
            try:
                self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':{'COL': 'x'}})
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #delete_row
            try:
                self.client_test.delete_row(table_name, Condition("IGNORE"), primary_keys)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #batch_get_row
            get_batches = [(table_name, [primary_keys], [])]
            try:
                self.client_test.batch_get_row(get_batches)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #batch_write_row
            put_row_item = PutRowItem(Condition("IGNORE"), primary_keys, {'COL': 'x'})
            update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys, {'put':{'COL': 'x'}})
            delete_row_item = DeleteRowItem(Condition("IGNORE"), primary_keys)
            batches_list = [
                {'table_name' : table_name, 'put' : [put_row_item]},
                {'table_name' : table_name, 'update' : [update_row_item]},
                {'table_name' : table_name, 'delete' : [delete_row_item]},
            ]
            for item in batches_list:
                write_batches = [item]
                try:
                    self.client_test.batch_write_row(write_batches)
                    self.assert_false()
                except OTSServiceError as e:
                    self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))

        #get_range
        def assert_get_range(table_name, inclusive_start_primary_keys, exclusive_end_primary_keys):
            try:
                self.client_test.get_range(table_name, 'FORWARD', inclusive_start_primary_keys, exclusive_end_primary_keys, [], None)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))


        pk_schema, exclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MAX)
        pk_schema, inclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MIN)
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = invalid_pk
            assert_get_range(table_name, inclusive_primary_keys, primary_keys)
            assert_get_range(table_name, primary_keys, exclusive_primary_keys)

    def test_pk_binary_value_with_max_length(self):
        """对于每个参数包含primary_keys的API，PK的string value长度为max，期望正常"""
        #create test table
        table_name = "table_test"
        pk_schema, valid_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "BINARY", "PK", bytearray(2))
        table_meta = TableMeta(table_name, pk_schema)
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')
        
        maxlength_pk = bytearray("x" * restriction.MaxPKStringValueLength)
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = maxlength_pk
            #get row
            consumed, pks, columns = self.client_test.get_row(table_name, primary_keys, [])
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(pks, {})
            self.assert_equal(columns,{})

            #batch_get_row
            get_batches = [(table_name, [primary_keys], [])]
            response = self.client_test.batch_get_row(get_batches)
            expect_row_data_item = RowDataItem(True, "", "", CapacityUnit(1, 0), {}, {})
            expect_response = [[expect_row_data_item]]
            self.assert_RowDataItem_equal(response, expect_response)
            
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = maxlength_pk
            #put_row
            consumed = self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, {'COL': 'x'})
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x'})))
            #update_row
            consumed = self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':{'COL': 'z'}})
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'z'})))
            #delete_row
            consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), primary_keys)
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'z'})))
            
            #batch_write_row
            put_row_item = PutRowItem(Condition("IGNORE"), primary_keys, {'COL': 'x'})
            update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys, {'put':{'COL': 'z'}})
            delete_row_item = DeleteRowItem(Condition("IGNORE"), primary_keys)

            op_list = [
                ('put', put_row_item),
                ('update', update_row_item),
                ('delete', delete_row_item),
            ]
            for op_type, op_item in op_list:
                write_batches = [{'table_name' : table_name, op_type : [op_item]}]
                response = self.client_test.batch_write_row(write_batches)
                expect_write_data_item = BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'z'})))
                expect_response = [{op_type : [expect_write_data_item]}]
                self.assert_BatchWriteRowResponseItem(response, expect_response) 

        #get_range
        def assert_get_range(table_name, inclusive_start_primary_keys, exclusive_end_primary_keys):
            consumed, next_start_primary_keys, rows = self.client_test.get_range(table_name, 'FORWARD', inclusive_start_primary_keys, exclusive_end_primary_keys, [], None)
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(next_start_primary_keys, None)
            self.assert_equal(rows, [])

        pk_schema, exclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MAX)
        pk_schema, inclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MIN)
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = maxlength_pk
            assert_get_range(table_name, inclusive_primary_keys, primary_keys)
            assert_get_range(table_name, primary_keys, exclusive_primary_keys)
 
    def test_pk_binary_value_empty(self):
        """BUG#268717 对于每个参数包含primary_keys的API，第一个PK的string value长度为0，期望正常"""
        #create test table
        table_name = "table_test"
        pk_schema, valid_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "BINARY", pk_name="PK", pk_value=bytearray(2))
        
        table_meta = TableMeta(table_name, pk_schema)
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')

        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = bytearray()
            #get row
            consumed, pks, columns = self.client_test.get_row(table_name, primary_keys, [])
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(pks, {})
            self.assert_equal(columns, {})        
        
            #batch_get_row
            get_batches = [(table_name, [primary_keys], [])]
            response = self.client_test.batch_get_row(get_batches)
            expect_row_data_item = RowDataItem(True, None, None, CapacityUnit(1, 0), {}, {})
            expect_response = [[expect_row_data_item]]
            self.assert_RowDataItem_equal(response, expect_response)
        
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = bytearray()
            #put_row
            consumed = self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, {'COL': 'x'})
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x'})))
            #update_row
            consumed = self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':{'COL': 'x1'}})
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x1'})))
            #delete_row
            consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), primary_keys)
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x1'})))

            #batch_write_row
            put_row_item = PutRowItem(Condition("IGNORE"), primary_keys, {'COL': 'x'})
            update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys, {'put':{'COL': 'z'}})
            delete_row_item = DeleteRowItem(Condition("IGNORE"), primary_keys)
            op_list = [
                ('put', put_row_item),
                ('update', update_row_item),
                ('delete', delete_row_item),
            ]
            for op_type, op_item in op_list:
                write_batches = [{'table_name' : table_name, op_type : [op_item]}]
                response = self.client_test.batch_write_row(write_batches)
                expect_write_data_item = BatchWriteRowResponseItem(True, None, None,  CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x'})))
                expect_response = [{op_type : [expect_write_data_item]}]
                self.assert_BatchWriteRowResponseItem(response, expect_response)
            
        #get_range
        def assert_get_range(table_name, inclusive_start_primary_keys, exclusive_end_primary_keys):
            consumed, next_start_primary_keys, rows = self.client_test.get_range(table_name, 'FORWARD', inclusive_start_primary_keys, exclusive_end_primary_keys, [], None)
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(next_start_primary_keys, None)
            self.assert_equal(rows, [])

        pk_schema, exclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MAX)
        pk_schema, inclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MIN)
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = bytearray()
            assert_get_range(table_name, inclusive_primary_keys, primary_keys)
            assert_get_range(table_name, primary_keys, exclusive_primary_keys)

    def test_pk_string_value_length_exceeded(self):
        """对于每个参数包含primary_keys的API，PK的string value长度为max + 1，期望返回ErrorCode: OTSParameterInvalid"""
        
        #create test table
        table_name = "table_test"
        pk_schema, valid_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING")

        table_meta = TableMeta(table_name, pk_schema)
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')
        
        invalid_pk = "x" * (restriction.MaxPKStringValueLength + 1)
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = invalid_pk
            #get row
            try:
                self.client_test.get_row(table_name, primary_keys, [])
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #put_row
            try:
                self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, {'COL': 'x'})
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #update_row
            try:
                self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':{'COL': 'x'}})
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #delete_row
            try:
                self.client_test.delete_row(table_name, Condition("IGNORE"), primary_keys)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #batch_get_row
            get_batches = [(table_name, [primary_keys], [])]
            try:
                self.client_test.batch_get_row(get_batches)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))
            #batch_write_row
            put_row_item = PutRowItem(Condition("IGNORE"), primary_keys, {'COL': 'x'})
            update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys, {'put':{'COL': 'x'}})
            delete_row_item = DeleteRowItem(Condition("IGNORE"), primary_keys)
            batches_list = [
                {'table_name' : table_name, 'put' : [put_row_item]},
                {'table_name' : table_name, 'update' : [update_row_item]},
                {'table_name' : table_name, 'delete' : [delete_row_item]},
            ]
            for item in batches_list:
                write_batches = [item]
                try:
                    self.client_test.batch_write_row(write_batches)
                    self.assert_false()
                except OTSServiceError as e:
                    self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))

        #get_range
        def assert_get_range(table_name, inclusive_start_primary_keys, exclusive_end_primary_keys):
            try:
                self.client_test.get_range(table_name, 'FORWARD', inclusive_start_primary_keys, exclusive_end_primary_keys, [], None)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", "The length of primary key column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % (pk, restriction.MaxPKStringValueLength, restriction.MaxPKStringValueLength + 1))


        pk_schema, exclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MAX)
        pk_schema, inclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MIN)
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = invalid_pk
            assert_get_range(table_name, inclusive_primary_keys, primary_keys)
            assert_get_range(table_name, primary_keys, exclusive_primary_keys)

    def test_pk_string_value_with_max_length(self):
        """对于每个参数包含primary_keys的API，PK的string value长度为max，期望正常"""
        #create test table
        table_name = "table_test"
        pk_schema, valid_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING")
        table_meta = TableMeta(table_name, pk_schema)
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')
        
        maxlength_pk = "x" * restriction.MaxPKStringValueLength
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = maxlength_pk
            #get row
            consumed, pks, columns = self.client_test.get_row(table_name, primary_keys, [])
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(pks, {})
            self.assert_equal(columns,{})

            #batch_get_row
            get_batches = [(table_name, [primary_keys], [])]
            response = self.client_test.batch_get_row(get_batches)
            expect_row_data_item = RowDataItem(True, "", "", CapacityUnit(1, 0), {}, {})
            expect_response = [[expect_row_data_item]]
            self.assert_RowDataItem_equal(response, expect_response)
            
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = maxlength_pk
            #put_row
            consumed = self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, {'COL': 'x'})
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x'})))
            #update_row
            consumed = self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':{'COL': 'z'}})
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'z'})))
            #delete_row
            consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), primary_keys)
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'z'})))
            
            #batch_write_row
            put_row_item = PutRowItem(Condition("IGNORE"), primary_keys, {'COL': 'x'})
            update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys, {'put':{'COL': 'z'}})
            delete_row_item = DeleteRowItem(Condition("IGNORE"), primary_keys)

            op_list = [
                ('put', put_row_item),
                ('update', update_row_item),
                ('delete', delete_row_item),
            ]
            for op_type, op_item in op_list:
                write_batches = [{'table_name' : table_name, op_type : [op_item]}]
                response = self.client_test.batch_write_row(write_batches)
                expect_write_data_item = BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'z'})))
                expect_response = [{op_type : [expect_write_data_item]}]
                self.assert_BatchWriteRowResponseItem(response, expect_response) 

        #get_range
        def assert_get_range(table_name, inclusive_start_primary_keys, exclusive_end_primary_keys):
            consumed, next_start_primary_keys, rows = self.client_test.get_range(table_name, 'FORWARD', inclusive_start_primary_keys, exclusive_end_primary_keys, [], None)
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(next_start_primary_keys, None)
            self.assert_equal(rows, [])

        pk_schema, exclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MAX)
        pk_schema, inclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MIN)
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = maxlength_pk
            assert_get_range(table_name, inclusive_primary_keys, primary_keys)
            assert_get_range(table_name, primary_keys, exclusive_primary_keys)
 
    def test_pk_string_value_empty(self):
        """BUG#268717 对于每个参数包含primary_keys的API，第一个PK的string value长度为0，期望正常"""
        #create test table
        table_name = "table_test"
        pk_schema, valid_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING")
        
        table_meta = TableMeta(table_name, pk_schema)
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')

        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = ""
            #get row
            consumed, pks, columns = self.client_test.get_row(table_name, primary_keys, [])
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(pks, {})
            self.assert_equal(columns, {})        
        
            #batch_get_row
            get_batches = [(table_name, [primary_keys], [])]
            response = self.client_test.batch_get_row(get_batches)
            expect_row_data_item = RowDataItem(True, None, None, CapacityUnit(1, 0), {}, {})
            expect_response = [[expect_row_data_item]]
            self.assert_RowDataItem_equal(response, expect_response)
        
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = ""
            #put_row
            consumed = self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, {'COL': 'x'})
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x'})))
            #update_row
            consumed = self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':{'COL': 'x1'}})
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x1'})))
            #delete_row
            consumed = self.client_test.delete_row(table_name, Condition("IGNORE"), primary_keys)
            self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x1'})))

            #batch_write_row
            put_row_item = PutRowItem(Condition("IGNORE"), primary_keys, {'COL': 'x'})
            update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys, {'put':{'COL': 'z'}})
            delete_row_item = DeleteRowItem(Condition("IGNORE"), primary_keys)
            op_list = [
                ('put', put_row_item),
                ('update', update_row_item),
                ('delete', delete_row_item),
            ]
            for op_type, op_item in op_list:
                write_batches = [{'table_name' : table_name, op_type : [op_item]}]
                response = self.client_test.batch_write_row(write_batches)
                expect_write_data_item = BatchWriteRowResponseItem(True, None, None,  CapacityUnit(0, self.sum_CU_from_row(primary_keys, {'COL': 'x'})))
                expect_response = [{op_type : [expect_write_data_item]}]
                self.assert_BatchWriteRowResponseItem(response, expect_response)
            
        #get_range
        def assert_get_range(table_name, inclusive_start_primary_keys, exclusive_end_primary_keys):
            consumed, next_start_primary_keys, rows = self.client_test.get_range(table_name, 'FORWARD', inclusive_start_primary_keys, exclusive_end_primary_keys, [], None)
            self.assert_consumed(consumed, CapacityUnit(1, 0))
            self.assert_equal(next_start_primary_keys, None)
            self.assert_equal(rows, [])

        pk_schema, exclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MAX)
        pk_schema, inclusive_primary_keys = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING", "PK", INF_MIN)
        for pk in valid_primary_keys.keys():
            primary_keys = copy.copy(valid_primary_keys)
            primary_keys[pk] = ""
            assert_get_range(table_name, inclusive_primary_keys, primary_keys)
            assert_get_range(table_name, primary_keys, exclusive_primary_keys)
    
    def _column_restriction_test_except(self, columns, error_code="", error_message=""):
        #create test table
        table_name = "table_test"
        table_meta = TableMeta(table_name, [('PK0', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit,restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')

        primary_keys = {"PK0": "x"}
        
        #put_row
        try:
            self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, columns)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, error_code, error_message)
        #update_row
        try:
            self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':columns})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, error_code, error_message)
        
        put_row_item = PutRowItem(Condition("IGNORE"), primary_keys, columns)
        update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys, {'put':columns})
        op_list = [
            ('put', put_row_item),
            ('update', update_row_item),
        ]
        for op_type, op_item in op_list:
            write_batches = [{'table_name' : table_name, op_type : [op_item]}]
            try:
                response = self.client_test.batch_write_row(write_batches)
                expect_write_data_item = BatchWriteRowResponseItem(False, error_code, error_message, None)
                expect_response = [{op_type : [expect_write_data_item]}]
                self.assert_BatchWriteRowResponseItem(response, expect_response) 
            except OTSServiceError as e:
                self.assert_error(e, 400, error_code, error_message)
   
    def test_column_string_value_length_exceeded(self):
        """对于每个参数包含column value的API，string value长度为max + 1，期望返回ErrorCode: OTSParameterInvalid"""
        self._column_restriction_test_except({'COL': 'x' * (restriction.MaxNonPKStringValueLength + 1)}, "OTSParameterInvalid", "The length of attribute column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % ('COL', restriction.MaxNonPKStringValueLength, restriction.MaxNonPKStringValueLength + 1)) 
        
    def test_column_string_value_with_max_length(self):
        """BUG#268717 对于每个参数包含column value的API，string value长度为max，期望正常"""
        self._column_restriction_test_normal({'COL': 'x' * restriction.MaxNonPKStringValueLength}) 

    def _column_restriction_test_normal(self, columns):
        #create_table
        table_name = "table_test"
        table_meta = TableMeta(table_name, [("PK0", "STRING")])
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit,restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')
        primary_keys = {"PK0": "x"}
        primary_keys1 = {"PK0": "x1"}

        #put_row
        consumed = self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, columns)
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, columns)))
        #update_row
        consumed = self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':columns})
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, columns)))
        #batch_put_row
        put_row_item = PutRowItem(Condition("IGNORE"), primary_keys1, columns)
        update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys1, {'put':columns})
        op_list = [
            ('put', put_row_item),
            ('update', update_row_item),
        ]
        for op_type, op_item in op_list:
            write_batches = [{'table_name' : table_name, op_type : [op_item]}]
            response = self.client_test.batch_write_row(write_batches)
            expect_write_data_item = BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, self.sum_CU_from_row(primary_keys1, columns)))
            expect_response = [{op_type : [expect_write_data_item]}]
            self.assert_BatchWriteRowResponseItem(response, expect_response)

    def test_column_string_value_empty(self):
        """BUG#268717 对于每个参数包含column value的API，string value长度为0，期望正常"""
        self._column_restriction_test_normal({"COL": ""})

    def test_column_binary_value_length_exceeded(self):
        """BUG#270021 对于每个参数包含column value的API，binary value长度为max + 1，期望返回ErrorCode: OTSParameterInvalid"""
        self._column_restriction_test_except({'COL': bytearray(restriction.MaxBinaryValueLength + 1)}, "OTSParameterInvalid", "The length of attribute column: '%s' exceeded the MaxLength:%d with CurrentLength:%d." % ('COL', restriction.MaxBinaryValueLength, restriction.MaxBinaryValueLength + 1))

    def test_column_binary_value_with_max_length(self):
        """BUG#270021 对于每个参数包含column value的API，binary value长度为max，期望正常"""
        self._column_restriction_test_normal({"COL": bytearray(restriction.MaxBinaryValueLength)})

    def test_column_binary_value_empty(self):
        """BUG#270021 对于每个参数包含column value的API，binary value长度为0，期望正常"""
        self._column_restriction_test_normal({"COL": bytearray(0)})

    def test_put_row_column_number_is_max(self):
        """对于PutRow和BatchWriteRow的PutRow操作，column的个数为max, total size < max，期望正常"""
        column_schema, columns = self.get_primary_keys(restriction.MaxColumnCountForRow, "STRING", "C", "x")
        self._column_restriction_test_normal(columns)

    def test_put_row_has_no_column(self):
        """BUG#269101 对于PutRow和BatchWriteRow的PutRow操作，column的个数为0，期望正常;update_row和batch_write_row的update_row操作，columns个数为0，返回异常"""
        columns = {}
        #create_table
        table_name = "table_test"
        table_meta = TableMeta(table_name, [("PK0", "STRING")])
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit,restriction.MaxReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test')
        primary_keys = {"PK0": "x"}

        #put_row
        consumed = self.client_test.put_row(table_name, Condition("IGNORE"), primary_keys, columns)
        self.assert_consumed(consumed, CapacityUnit(0, self.sum_CU_from_row(primary_keys, columns)))
        #batch_put_row
        put_row_item = PutRowItem(Condition("IGNORE"), primary_keys, columns)
        write_batches = [{'table_name' : table_name, 'put' : [put_row_item]}]
        response = self.client_test.batch_write_row(write_batches)
        expect_write_data_item = BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, self.sum_CU_from_row(primary_keys, columns)))
        expect_response = [{'put' : [expect_write_data_item]}]
        self.assert_BatchWriteRowResponseItem(response, expect_response)

        #update_row
        try:
            self.client_test.update_row(table_name, Condition("IGNORE"), primary_keys, {'put':columns})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "No column specified while updating row.")
        #batch_update_row
        update_row_item = UpdateRowItem(Condition("IGNORE"), primary_keys, {'put':columns})
        write_batches = [{'table_name' : table_name, 'update' : [update_row_item]}]
        try:
            response = self.client_test.batch_write_row(write_batches)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "No attribute column specified to update row #%d in table: '%s'." % (0, table_name))

            
    ##########################################################################################################################
    # 付哲请从这里开始

    def test_put_row_columns_total_size_is_max(self):
        """BUG#268859   对于PutRow, UpdateRow和BatchWriteRow的相关操作, columns总大小为max byte, number < max, 期望正常"""
        self._create_table_with_4_pk('XX')
        pks, cols = self._create_maxsize_row()
        ewcu = self.sum_CU_from_row(pks, cols)

        self.assert_consumed(CapacityUnit(0, ewcu), self.client_test.put_row('XX', Condition("IGNORE"), pks, cols))
        cu, rpks, rcols = self.client_test.get_row('XX', pks)
        self.assert_consumed(cu, CapacityUnit(ewcu, 0))
        self.assert_equal(rpks, pks)
        self.assert_equal(rcols, cols)

        self.assert_consumed(CapacityUnit(0, ewcu), self.client_test.update_row('XX', Condition("IGNORE"), pks, {'put':cols}))
        cu, rpks, rcols = self.client_test.get_row('XX', pks)
        self.assert_consumed(cu, CapacityUnit(ewcu, 0))
        self.assert_equal(rpks, pks)
        self.assert_equal(rcols, cols)

        putrow_item = PutRowItem(Condition("IGNORE"), pks, cols)
        response = self.client_test.batch_write_row([{'table_name': 'XX', 'put': [putrow_item]}])
        eresponse = [{'put': [BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, ewcu))]}]
        self.assert_BatchWriteRowResponseItem(response, eresponse)
        cu, rpks, rcols = self.client_test.get_row('XX', pks)
        self.assert_consumed(cu, CapacityUnit(ewcu, 0))
        self.assert_equal(rpks, pks)
        self.assert_equal(rcols, cols)

        updaterow_item = UpdateRowItem(Condition("IGNORE"), pks, {'put':cols})
        response = self.client_test.batch_write_row([{'table_name': 'XX', 'update': [updaterow_item]}])
        eresponse = [{'update': [BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, ewcu))]}]
        self.assert_BatchWriteRowResponseItem(response, eresponse)
        cu, rpks, rcols = self.client_test.get_row('XX', pks)
        self.assert_consumed(cu, CapacityUnit(ewcu, 0))
        self.assert_equal(rpks, pks)
        self.assert_equal(rcols, cols)


    def test_capacity_unit_exceeded(self):
        """对于CreateTable和UpdateTable操作，（read = 5000, write = 5001）或者 (read = 5001, write = 5000)，期望返回ErrorCode: OTSTooManyCapacityUnits"""
        table_meta = TableMeta('XX', [('PK0', 'STRING')])                
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit + 1, 
            restriction.MaxReadWriteCapacityUnit
        ))
        err_str = '[' + str(restriction.MinReadWriteCapacityUnit) + ', ' + str(restriction.MaxReadWriteCapacityUnit) + '].'

        try:
            self.client_test.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")

        reserved_throughput2 = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit + 1
        ))
        try:
            self.client_test.create_table(table_meta, reserved_throughput2)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")

        self.client_test.create_table(table_meta, ReservedThroughput(CapacityUnit(1, 1)))
        try:
            self.client_test.update_table('XX', reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")

        try:
            self.client_test.update_table('XX', reserved_throughput2)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")

    def test_capacity_unit_is_max(self):
        """对于CreateTable和UpdateTable操作， Capacity Unit (read = 5000, write = 5000)期望正常"""
        table_meta = TableMeta('XX', [('PK0', 'STRING')])
        capacity_unit = CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit
        )
        reserved_throughput = ReservedThroughput(capacity_unit)
        self.client_test.create_table(table_meta, reserved_throughput)

        time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
        update_response = UpdateTableResponse(ReservedThroughputDetails(capacity_unit, time.time(), None, 0))
        self.assert_UpdateTableResponse(
                self.client_test.update_table('XX', reserved_throughput), 
                update_response)
    
    def test_capacity_unit_lower_limit(self):
        """对于CreateTable和UpdateTable操作， Capacity Unit (read = 1, write = 1)期望正常"""
        table_meta = TableMeta('XX', [('PK0', 'STRING')])
        capacity_unit = CapacityUnit(
            restriction.MinReadWriteCapacityUnit, 
            restriction.MinReadWriteCapacityUnit
        )
        reserved_throughput = ReservedThroughput(capacity_unit)
        self.client_test.create_table(table_meta, reserved_throughput)
        time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
        update_response = UpdateTableResponse(ReservedThroughputDetails(capacity_unit, time.time(), None, 0))

        self.assert_UpdateTableResponse(
                self.client_test.update_table('XX', reserved_throughput), 
                update_response)

    def test_multi_get_row_number_exceeded(self):
        """MultiGetRow中包含的row个数为max + 1，期望返回ErrorCode: OTSParameterInvalid"""
        table_meta = TableMeta('XX', [('PK0', 'STRING')])                
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit
        ))
        self.client_test.create_table(table_meta, reserved_throughput)

        rows = []
        for i in range(0, restriction.MaxRowCountForMultiGetRow + 1):
            rows.append({'PK0': str(i)})

        try:
            self.client_test.batch_get_row([('XX', rows, [])])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "Rows count exceeds the upper limit")

    def test_multi_get_row_number_is_max(self):
        """MultiGetRow中包含的row个数为max，期望正常"""
        table_meta = TableMeta('XX', [('PK0', 'STRING')])                
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit
        ))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('XX')

        rows = []
        expect_response = []
        for i in range(0, restriction.MaxRowCountForMultiGetRow):
            pks = {'PK0': str(i)}
            rows.append(pks)
            row_data_item = RowDataItem(True, '', '', CapacityUnit(1, 0), {}, {})
            expect_response.append(row_data_item)

        response = self.client_test.batch_get_row([('XX', rows, [])])
        self.assert_RowDataItem_equal(response, [expect_response])

    def test_multi_write_row_number_exceeded(self):
        """MultiWriteRow中包含的row个数为max + 1，期望返回ErrorCode: OTSParameterInvalid"""
        table_meta = TableMeta('XX', [('PK0', 'STRING')])                
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit
        ))
        self.client_test.create_table(table_meta, reserved_throughput)

        rows = []
        for i in range(0, restriction.MaxRowCountForMultiWriteRow + 1):
            put_item = PutRowItem(Condition("IGNORE"), {'PK0': str(i)}, {})
            rows.append(put_item)

        try:
            self.client_test.batch_write_row([{'table_name': 'XX', 'put': rows}])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "Rows count exceeds the upper limit")

        rows = []
        for i in range(0, restriction.MaxRowCountForMultiWriteRow + 1):
            put_item = UpdateRowItem(Condition("IGNORE"), {'PK0': str(i)}, {})
            rows.append(put_item)

        try:
            self.client_test.batch_write_row([{'table_name': 'XX', 'update': rows}])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "Rows count exceeds the upper limit")

        rows = []
        for i in range(0, restriction.MaxRowCountForMultiWriteRow + 1):
            put_item = DeleteRowItem(Condition("IGNORE"), {'PK0': str(i)})
            rows.append(put_item)

        try:
            self.client_test.batch_write_row([{'table_name': 'XX', 'delete': rows}])
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "Rows count exceeds the upper limit")

    def test_multi_write_row_number_is_max(self):
        """BUG#268859   MultiWriteRow中包含的row个数为max，期望正常"""
        table_meta = TableMeta('XX', [('PK0', 'STRING')])                
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit
        ))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('XX')

        rows = []
        expect_response = []
        for i in range(0, restriction.MaxRowCountForMultiGetRow):
            put_item = PutRowItem(Condition("IGNORE"), {'PK0': str(i)}, {})
            rows.append(put_item)
            response_item = BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, 1))
            expect_response.append(response_item)

        response = self.client_test.batch_write_row([{'table_name': 'XX', 'put': rows}])
        eresponse = [{'put': expect_response}]
        self.assert_BatchWriteRowResponseItem(response, eresponse)

        rows = []
        expect_response = []
        for i in range(0, restriction.MaxRowCountForMultiGetRow):
            pks = {'PK0': str(i)}
            update_item = UpdateRowItem(Condition("IGNORE"), pks, {'put':{'C1': 'V'}})
            rows.append(update_item)
            response_item = BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, 1))
            expect_response.append(response_item)

        response = self.client_test.batch_write_row([{'table_name': 'XX', 'update': rows}])
        eresponse = [{'update': expect_response}]
        self.assert_BatchWriteRowResponseItem(response, eresponse)

        rows = []
        expect_response = []
        for i in range(0, restriction.MaxRowCountForMultiGetRow):
            pks = {'PK0': str(i)}
            delete_item = DeleteRowItem(Condition("IGNORE"), pks)
            rows.append(delete_item)
            response_item = BatchWriteRowResponseItem(True, "", "", CapacityUnit(0, 1))
            expect_response.append(response_item)

        response = self.client_test.batch_write_row([{'table_name': 'XX', 'delete': rows}])
        eresponse = [{'delete': expect_response}]
        self.assert_BatchWriteRowResponseItem(response, eresponse)

    def test_get_range_rows_number_too_much_for_one_return(self):
        """BUG#269084   一个表中包含max + 1行，total size < max，GetRange读取期望2次完成"""
        table_meta = TableMeta('XX', [('PK0', 'INTEGER')])                
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit
        ))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('XX')

        size = 0
        response_rows = []
        for i in range(0, restriction.MaxRowCountForGetRange + 1):
            pks = {'PK0': i}
            size += self.get_row_size(pks, {})
            if i < restriction.MaxRowCountForGetRange:
                response_rows.append((pks, {}))
            self.client_test.put_row('XX', Condition("IGNORE"), pks, {})
        
        cu, next_start_pks, rows = self.client_test.get_range('XX', 'FORWARD', {'PK0': INF_MIN}, {'PK0': INF_MAX})
        self.assert_consumed(cu, CapacityUnit(int(math.ceil(size * 1.0 / 4096)) , 0))
        self.assert_equal(len(rows), restriction.MaxRowCountForGetRange)
        self.assert_equal(rows, response_rows)
        self.assert_equal(next_start_pks, {'PK0': restriction.MaxRowCountForGetRange})

        cu, next_start_pks, rows = self.client_test.get_range('XX', 'FORWARD', next_start_pks, {'PK0': INF_MAX})
        self.assert_equal(next_start_pks, None)

    def test_get_range_rows_number_just_enough_for_one_return(self):
        """BUG#269084   一个表中包含max行，total size < max，GetRange读取期望1次完成"""
        table_meta = TableMeta('XX', [('PK0', 'INTEGER')])                
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit
        ))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('XX')

        size = 0
        response_rows = []
        for i in range(0, restriction.MaxRowCountForGetRange):
            pks = {'PK0': i}
            size += self.get_row_size(pks, {})
            response_rows.append((pks, {}))
            self.client_test.put_row('XX', Condition("IGNORE"), pks, {})

        cu, next_start_pks, rows = self.client_test.get_range('XX', 'FORWARD', {'PK0': INF_MIN}, {'PK0': INF_MAX})
        self.assert_consumed(cu, CapacityUnit(int(math.ceil(size * 1.0 / 4096)), 0))
        self.assert_equal(len(rows), restriction.MaxRowCountForGetRange)
        self.assert_equal(rows, response_rows)
        self.assert_equal(next_start_pks, None)

    def test_get_range_rows_size_too_much_for_one_return(self):
        """BUG#268997  BUG#269084    一个表中包含max + 1 byte数据，行个数 < max，GetRange读取期望2次完成"""
        self._create_table_with_4_pk('XX')
        size = 0
        for i in range(0, restriction.MaxDataSizeForGetRange / restriction.MaxColumnDataSizeForRow):
            pks, columns = self._create_maxsize_row(str(i))
            if i == 0:
                pks['PK0'] = '0' * (len(pks['PK0']) - 15)
            size += self.get_row_size(pks, columns)
            self.client_test.put_row('XX', Condition("IGNORE"), pks, columns)
        print "size = %d" % size 
        pks = {'PK0': 'a', 'PK1': 'b', 'PK2': 'c', 'PK3': 'd'}
        self.client_test.put_row('XX', Condition("IGNORE"), pks, {})

        start_pks = {'PK0': INF_MIN, 'PK1': INF_MIN, 'PK2': INF_MIN, 'PK3': INF_MIN}
        end_pks = {'PK0': INF_MAX, 'PK1': INF_MAX, 'PK2': INF_MAX, 'PK3': INF_MAX}

        cu, next_start_pks, rows = self.client_test.get_range('XX', 'FORWARD', start_pks, end_pks)
        self.assert_consumed(cu, CapacityUnit(int(math.ceil(size * 1.0 / 4096)), 0))
        self.assert_equal(next_start_pks != None, True)

        cu, next_start_pks, rows = self.client_test.get_range('XX', 'FORWARD', next_start_pks, end_pks)
        self.assert_consumed(cu, CapacityUnit(1, 0))
        self.assert_equal(len(rows), 1)
        self.assert_equal(rows[0], (pks, {}))
        self.assert_equal(next_start_pks, None)

    def test_get_range_rows_size_just_enough_for_one_return(self):
        """BUG#269084   一个表中包含max byte数据, 行个数 < max，GetRange读取期望1次完成"""
        self._create_table_with_4_pk('XX')
        size = 0
        for i in range(0, restriction.MaxDataSizeForGetRange / restriction.MaxColumnDataSizeForRow):
            pks, columns = self._create_maxsize_row(str(i))
            size += self.get_row_size(pks, columns)
            self.client_test.put_row('XX', Condition("IGNORE"), pks, columns)

        start_pks = {'PK0': INF_MIN, 'PK1': INF_MIN, 'PK2': INF_MIN, 'PK3': INF_MIN}
        end_pks = {'PK0': INF_MAX, 'PK1': INF_MAX, 'PK2': INF_MAX, 'PK3': INF_MAX}

        cu, next_start_pks, rows = self.client_test.get_range('XX', 'FORWARD', start_pks, end_pks)
        self.assert_consumed(cu, CapacityUnit(int(math.ceil(size * 1.0 / 4096)), 0))
        self.assert_equal(next_start_pks, None)

    #############################################################################################################
    # 新加的
    def test_create_table_failure_doesnot_count_as_succeeded(self):
        """新建max - 1个表，然后创建一个TableMeta错的表失败，再新建一个表，期望成功"""
        count = 1
        while count <= restriction.MaxTableCountForInstance - 1:
            table_name = 'table_test_max_num' + str(count)
            table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
            reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
            self.client_test.create_table(table_meta, reserved_throughput)
            count = count + 1

        table_name = 'table_test_max_num' + str(count)
        table_meta = TableMeta(table_name, [('PK0', 'DOUBLE'), ('PK1', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
        try:
            self.client_test.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "DOUBLE is an invalid type for the primary key.")

        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(10,10))
        self.client_test.create_table(table_meta, reserved_throughput)


    #############################################################################################################
    #bug261240 前端限制单行每次操作的列数
    def test_column_num_limit_put_row(self):
        '''构造单行列数刚好为限制列数的PutRow请求，期望请求成功，构造单行列数为限制列数+1的PutRow请求，
        期望返回OTSParameterInvalidException。'''
        max_column_num = restriction.MaxColumnCountForRow 
        table_name = 'table_test_column_num'
        table_meta = TableMeta(table_name, [('PK', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load(table_name)
        columns = {"COL" + str(x) : x for x in xrange(max_column_num)}
        self.client_test.put_row(table_name, Condition("IGNORE"), {"PK": 1}, columns)
        columns = {"COL" + str(x) : x for x in xrange(max_column_num + 1)}
        try:
            self.client_test.put_row(table_name, Condition("IGNORE"), {"PK": 1}, columns)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit.")

    def test_column_num_limit_update_row(self):
        '''分三种情况构造单行列数为限制列数和单行列数为限制列数+1的UpdateRow请求，
        三种情况分别是只添加、有添加和修改、有更新修改和删除。'''
        max_column_num = restriction.MaxColumnCountForRow
        condition = Condition("IGNORE")
        table_name = 'table_test_column_num'
        table_meta = TableMeta(table_name, [('PK', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load(table_name)
        #UpdateRow只添加：
        columns = {"COL" + str(x) : x for x in xrange(max_column_num + 1)}
        try: 
            self.client_test.update_row(table_name, condition, {"PK": 1}, {"put": columns})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit.")
        columns = {"COL" + str(x) : x for x in xrange(max_column_num)}
        self.client_test.update_row(table_name, condition, {"PK": 1}, {"put": columns})
        #UpdateRow有添加和修改:
        pre_put_num = max_column_num / 2 
        columns = {"COL" + str(x) : x for x in xrange(pre_put_num)}
        self.client_test.update_row(table_name, condition, {"PK": 2}, {"put": columns})
        columns = {"COL" + str(x) : x for x in xrange(max_column_num + 1)}
        try: 
            self.client_test.update_row(table_name, condition, {"PK": 2}, {"put": columns})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit.")
        columns = {"COL" + str(x) : x for x in xrange(max_column_num)}
        self.client_test.update_row(table_name, condition, {"PK": 2}, {"put": columns})
        #UpdateRow有添加、修改和删除:
        pre_put_num = max_column_num * 2 / 3;
        columns = {"COL" + str(x) : x for x in xrange(pre_put_num)}
        self.client_test.update_row(table_name, condition, {"PK": 3}, {"put": columns})
        columns_delete = ["COL" + str(x) for x in xrange(max_column_num / 3)]
        columns_put = {"COL" + str(x) : x for x in xrange(max_column_num / 3, max_column_num + 1)}
        try: 
            self.client_test.update_row(table_name, condition, {"PK": 3}, {"put": columns_put, "delete": columns_delete})
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit.")
        columns_put = {"COL" + str(x) : x for x in xrange(max_column_num / 3, max_column_num)}
        self.client_test.update_row(table_name, condition, {"PK": 3}, {"put": columns_put, "delete": columns_delete})

    def test_column_num_limit_batch_write_row(self):
        '''1.构造多表多行的BatchWriteRow请求 (包含PutRow、UpdateRow、DeleteRow) ，当所有PutRow和UpdateRow行的列数都刚好为限制列数，UpdateRow的行包含三种情况（只更新，有更新和修改，有更新修改和删除）时，期望请求成功。
           2.分别测试“只包含PutRow”、“只包含UpdateRow的一种情况”、“包含UpdateRow的三种情况”的BatchWriteRow，且列数为限制列数+1，期望返回 OTSParameterInvalidException。'''
        max_column_num = restriction.MaxColumnCountForRow 
        condition = Condition("IGNORE")
        table_name_1 = 'table_test_column_num_1'
        table_meta_1 = TableMeta(table_name_1, [('PK', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
        self.client_test.create_table(table_meta_1, reserved_throughput)
        table_name_2 = 'table_test_column_num_2'
        table_meta_2 = TableMeta(table_name_2, [('PK', 'INTEGER')])
        self.client_test.create_table(table_meta_2, reserved_throughput)
        self.wait_for_partition_load(table_name_1)
        self.wait_for_partition_load(table_name_2)
        columns = {"COL" + str(x) : x for x in xrange(max_column_num / 2)}
        self.client_test.update_row(table_name_1, condition, {"PK": 1}, {"put": columns})
        columns = {"COL" + str(x) : x for x in xrange(max_column_num * 2 / 3)}
        self.client_test.update_row(table_name_1, condition, {"PK": 2}, {"put": columns})

        columns = {"COL" + str(x) : x for x in xrange(max_column_num + 1)}
        put_row_item      = PutRowItem(condition, {"PK": 1}, columns)
        update_row_item_1 = UpdateRowItem(condition, {"PK": 3}, {"put": columns})
        update_row_item_2 = UpdateRowItem(condition, {"PK": 1}, {"put": columns})
        
        columns_delete = ["COL" + str(x) for x in xrange(max_column_num / 3)]
        columns_put = {"COL" + str(x) : x for x in xrange(max_column_num / 3, max_column_num + 1)}
        update_row_item_3 = UpdateRowItem(condition, {"PK": 2}, {"put": columns_put, "delete": columns_delete})
        delete_row_item = DeleteRowItem(condition, {"PK": 4})

        try:
            table_item_1 = {'table_name': 'table_test_column_num_1', 
                            'put': [put_row_item], 
                            'update': [update_row_item_2, update_row_item_3]}
            table_item_2 = {'table_name': 'table_test_column_num_2', 
                             'update': [update_row_item_1],
                            'delete': [delete_row_item]}
            batch_list = [table_item_1, table_item_2]
            self.client_test.batch_write_row(batch_list)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit of putting row #0 in table: 'table_test_column_num_1'.")
        try:
            table_item = {'table_name': 'table_test_column_num_1', 'put': [put_row_item]}
            batch_list = [table_item]
            self.client_test.batch_write_row(batch_list)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit of putting row #0 in table: 'table_test_column_num_1'.")
        try:
            table_item = {'table_name': 'table_test_column_num_1', 'update': [update_row_item_1]}
            batch_list = [table_item]
            self.client_test.batch_write_row(batch_list)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit of updating row #0 in table: 'table_test_column_num_1'.")
        try:
            table_item = {'table_name': 'table_test_column_num_1', 'update': [update_row_item_2]}
            batch_list = [table_item]
            self.client_test.batch_write_row(batch_list)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit of updating row #0 in table: 'table_test_column_num_1'.")
        try:
            table_item = {'table_name': 'table_test_column_num_1', 'update': [update_row_item_3]}
            batch_list = [table_item]
            self.client_test.batch_write_row(batch_list)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit of updating row #0 in table: 'table_test_column_num_1'.")
        
        columns = {"COL" + str(x) : x for x in xrange(max_column_num)}
        update_row_item_1 = UpdateRowItem(condition, {"PK": 1}, {"put": columns})
        columns_delete = ["COL" + str(x) for x in xrange(max_column_num / 3)]
        columns_put = {"COL" + str(x) : x for x in xrange(max_column_num / 3, max_column_num)}
        update_row_item_2 = UpdateRowItem(condition, {"PK": 2}, {"put": columns_put, "delete": columns_delete})
        delete_row_item = DeleteRowItem(condition, {"PK": 3})
        table_item_1 = {'table_name': 'table_test_column_num_1', 'update': [update_row_item_1, update_row_item_2], 
                        'delete': [delete_row_item]}
        put_row_item = PutRowItem(condition, {"PK": 1}, columns)
        update_row_item = UpdateRowItem(condition, {"PK": 2}, {"put": columns})
        table_item_2 = {'table_name': 'table_test_column_num_2', 'put': [put_row_item], 'update': [update_row_item]}
        batch_list = [table_item_1, table_item_2]
        self.client_test.batch_write_row(batch_list)

    def test_columns_to_get_num_limit(self):
        '''分别构造columns_to_get个数刚好为限制列数的GetRow，BatchGetRow和GetRange的请求，期望请求成功；
           分别构造columns_to_get个数刚好为限制列数+1的GetRow，BatchGetRow和GetRange的请求，期望返回OTSParameterInvalidException。'''
        max_column_num = restriction.MaxColumnCountForRow 
        condition = Condition("IGNORE")
        table_name = 'table_test_column_num'
        table_meta = TableMeta(table_name, [('PK', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(100, 100))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load(table_name)
        columns = {"COL" + str(x) : x for x in xrange(max_column_num)}
        self.client_test.put_row(table_name, Condition("IGNORE"), {"PK": 1}, columns)
        columns_to_get = ["COL" + str(x) for x in xrange(max_column_num)]
        self.client_test.get_row(table_name, {"PK": 1}, columns_to_get)
        batch_list = [(table_name, [{"PK": 1}], columns_to_get)]
        self.client_test.batch_get_row(batch_list)
        self.client_test.get_range(table_name, 'FORWARD', {"PK":INF_MIN}, {"PK":INF_MAX}, columns_to_get)
        
        columns_to_get = ["COL" + str(x) for x in xrange(max_column_num + 1)]
        try:
            self.client_test.get_row(table_name, {"PK": 1}, columns_to_get)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit.")
        try:
            batch_list = [(table_name, [{"PK": 1}], columns_to_get)]
            self.client_test.batch_get_row(batch_list)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit of getting row in table 'table_test_column_num'.")
        try:
            self.client_test.get_range(table_name, 'FORWARD', {"PK":INF_MIN}, {"PK":INF_MAX}, columns_to_get)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "The number of columns from the request exceeded the limit.")
