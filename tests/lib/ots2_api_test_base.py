#*-coding:utf-8-*-
import subprocess

from unittest import TestCase
import ots2_api_test_config
import global_test_config
import global_test_lib
from ots2 import * 
from ots2.error import * 
from ots2.retry import *
import atest.log
import atest.auto as auto
from atest.auto import AutoCmdRetcodeError
import types
import math
import time
import restriction
import traceback
import commands
import sys 
from ocm_client import *

import os 
import inspect

class OTS2APITestBase(TestCase):

    def __init__(self, methodName=None):
        TestCase.__init__(self, methodName=methodName)
        self.logger = atest.log.root
        self.start_time = 0

    def _check_ots_server_alive(self, host):
        try:
            auto.run('ssh %s "ps ax | grep ots_server | grep -v grep"' % host)
            return True
        except AutoCmdRetcodeError as e:
            return False

    def setUp(self):

        self.client_test = OTSClient(
            ots2_api_test_config.endpoint,
            ots2_api_test_config.access_id,
            ots2_api_test_config.access_key,
            ots2_api_test_config.instance_name_for_common,
            logger_name=atest.log.root.name,
            retry_policy=NoRetryPolicy(),
        )
        
        for table_name in self.client_test.list_table():
            self.client_test.delete_table(table_name)

    def tearDown(self):
        pass

    def case_post_check(self):
        pass
    
    def assert_error(self, error, http_status, error_code, error_message):
        self.assert_equal(error.http_status, http_status)
        self.assert_equal(error.code, error_code)
        self.assert_equal(error.message, error_message)

    def assert_false(self):
        self.logger.warn("\nAssertion Failed\n" + "".join(traceback.format_stack()))
        raise AssertionError

    def assert_equal(self, res, expect_res):
        if res != expect_res:
            self.logger.warn("\nAssertion Failed\nactual: %s\nexpect: %s\n" % (res, expect_res) + "".join(traceback.format_stack()))
            self.assertEqual(res, expect_res)

    def try_exhaust_cu(self, func, count, read_cu, write_cu):
        i = 0
        while True:
            try:
                self._try_exhaust_cu(func, count, read_cu, write_cu)
                break
            except Exception as e:
                i += 1
                self.logger.info("_try_exhaust_cu failed: %s" % str(e))
                if i >= 10:
                    self.assert_false()

    def _try_exhaust_cu(self, func, count, read_cu, write_cu):
        start_time = time.time()
        read_cu_sum = 0
        write_cu_sum = 0
        max_elapsed_time = 1.0 / (read_cu if read_cu != 0 else write_cu) * count
        self.logger.info("StartTime: %s, Count: %s, ReadCU: %s, WriteCU: %s, MaxElapsedTime: %s" % (start_time, count, read_cu, write_cu, max_elapsed_time));

        while count != 0:
            try:
                rc, wc = func()
                read_cu_sum += rc
                write_cu_sum += wc
                count -= 1
                self.logger.info("ReadCU: %s, WriteCU: %s, ReadCUSum: %s, WriteCUSum: %s, Count: %s" % (rc, wc, read_cu_sum, write_cu_sum, count))
            except OTSServiceError as e:
                self.assert_error(e, 403, "OTSNotEnoughCapacityUnit", "Remaining capacity unit is not enough.")
        
        end_time = time.time()
        interval = end_time - start_time
        if interval >= max_elapsed_time * 1.2:
                raise Exception('Exceed max elapsed_time: %s, %s' % (interval, max_elapsed_time)) 
        avg_read_cu = read_cu_sum / interval
        avg_write_cu = write_cu_sum / interval
        self.logger.info("Interval: %s, AvgReadCU: %s, AvgWriteCU: %s, ReadCU: %s, WriteCU: %s" % (interval, avg_read_cu, avg_write_cu, read_cu, write_cu))

        if read_cu != 0:
            self.assertTrue(avg_read_cu >= read_cu * 0.8)
            self.assertTrue(avg_read_cu < read_cu * 1.2)

        if write_cu != 0:
            self.assertTrue(avg_write_cu >= write_cu * 0.8)
            self.assertTrue(avg_write_cu < write_cu * 1.2)
 
    def try_to_consuming(self, table_name, pk_dict_exist, pk_dict_not_exist,
                         capacity_unit): 
        read = capacity_unit.read  
        write = capacity_unit.write 
        no_check_flag = 0
        if read > 1 or write > 1:
            read = read - 1
            write = write - 1
            no_check_flag = 1
        columns = {}
        column_value_size = 4096
        all_pk_length = self.get_row_size(pk_dict_exist, {})
        #write
        for i in range(write):
            if i is not 0:
                columns['X' * i] = 'X' * (column_value_size - i)
            else:
                columns['col0'] = 'X' * (column_value_size - all_pk_length - 10)
        if write is not 0:
            consumed_update = self.client_test.update_row(table_name, Condition("IGNORE"), pk_dict_exist, {'put':columns})
            expect_consumed = CapacityUnit(0, self.sum_CU_from_row(pk_dict_exist, columns))
            self.assert_consumed(consumed_update, expect_consumed)
            self.assert_equal(write, self.sum_CU_from_row(pk_dict_exist, columns))
        #consume(0, 1)
        if 1 == no_check_flag: 
            try:
                consumed_update = self.client_test.delete_row(table_name, Condition("IGNORE"), pk_dict_not_exist)
            except OTSServiceError as e:
                self.assert_false()
        
        #read
        while read >= write and write != 0:
            read = read - write
            consumed_read, primary_keys, columns_get_row = self.client_test.get_row(table_name, pk_dict_exist)
            expect_consumed = CapacityUnit(self.sum_CU_from_row(pk_dict_exist, columns), 0)
            self.assert_consumed(consumed_read, expect_consumed)
            self.assert_equal(primary_keys, pk_dict_exist)
        for i in range(read + no_check_flag):
            consumed_read, primary_keys, columns_get_row = self.client_test.get_row(table_name, pk_dict_not_exist)
            self.assert_consumed(consumed_read, CapacityUnit(1, 0))
            self.assert_equal(primary_keys, {})

    def check_CU_by_consuming(self, table_name, pk_dict_exist, pk_dict_not_exist, 
                              capacity_unit):  
        begin_time = time.time()
        self.try_to_consuming(table_name, pk_dict_exist, pk_dict_not_exist, capacity_unit)
        #只在CU较小进行强验证
        if capacity_unit.write <= 1 and capacity_unit.read <= 1:
            #consume(0, 1)
            try:
                consumed_update = self.client_test.delete_row(table_name, Condition("IGNORE"), pk_dict_not_exist)
                end_time = time.time()
                if end_time - begin_time < 1:
                    self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 403, "OTSNotEnoughCapacityUnit", "Remaining capacity unit for write is not enough.")
            #consume(1, 0)
            try:
                consumed_read, primary_keys, columns_get_row = self.client_test.get_row(table_name, pk_dict_not_exist)
                end_time = time.time()
                if end_time - begin_time < 1:
                    self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 403, "OTSNotEnoughCapacityUnit", "Remaining capacity unit for read is not enough.")

    def assert_consumed(self, consumed, expect_consumed):
        if consumed == None or expect_consumed == None:
            self.assert_equal(consumed, expect_consumed)
        else:
            self.assert_equal(consumed.read, expect_consumed.read)
            self.assert_equal(consumed.write, expect_consumed.write)
 
    def assert_RowDataItem_equal(self, response, expect_response):
        self.assert_equal(len(response), len(expect_response))
        for i in range(len(response)):
            self.assert_equal(len(response[i]), len(expect_response[i]))
            for j in range(len(response[i])):
            
                if expect_response[i][j].is_ok and not response[i][j].is_ok:
                    raise Exception("BatchGetRow failed on at least one row, ErrorCode: %s ErrorMessage: %s" % (response[i][j].error_code, response[i][j].error_message))

                self.assert_equal(response[i][j].is_ok, expect_response[i][j].is_ok)
                if expect_response[i][j].is_ok:
                    self.assert_consumed(response[i][j].consumed, expect_response[i][j].consumed)
                    self.assert_equal(response[i][j].primary_key_columns, expect_response[i][j].primary_key_columns)
                    self.assert_equal(response[i][j].attribute_columns, expect_response[i][j].attribute_columns)
                else:
                    self.assert_equal(response[i][j].error_code, expect_response[i][j].error_code)
                    self.assert_equal(response[i][j].error_message, expect_response[i][j].error_message)
                    self.assert_consumed(response[i][j].consumed, expect_response[i][j].consumed)

    def assert_BatchWriteRowResponseItem(self, response, expect_response):
        self.assert_equal(len(response), len(expect_response))
        item_list = ['put', 'update', 'delete']
        for i in range(len(response)):
            for item in item_list:
                if response[i].has_key(item) == expect_response[i].has_key(item):
                    if response[i].has_key(item):
                        self.assert_equal(len(response[i][item]), len(expect_response[i][item]))

                        for j in range(len(response[i][item])):
                            if expect_response[i][item][j].is_ok and not response[i][item][j].is_ok:
                                raise Exception("BatchWriteRow failed on at least one row, ErrorCode: %s ErrorMessage: %s" % (response[i][item][j].error_code, response[i][item][j].error_message))
                            self.assert_equal(response[i][item][j].is_ok, expect_response[i][item][j].is_ok)

                            if expect_response[i][item][j].is_ok:
                                self.assert_consumed(response[i][item][j].consumed, expect_response[i][item][j].consumed)
                            else:
                                self.assert_equal(response[i][item][j].error_code, expect_response[i][item][j].error_code)
                                self.assert_equal(response[i][item][j].error_message, expect_response[i][item][j].error_message)
                                self.assert_consumed(response[i][item][j].consumed, expect_response[i][item][j].consumed)
                else:
                    self.assert_false()

    def assert_time(self, res_time, expect_time, delta=60):
        if res_time == None or expect_time == None:
            self.assert_equal(res_time, expect_time)
        else:
            self.assert_equal((max(res_time, expect_time) - min(res_time, expect_time) <= delta), True)

    def assert_ReservedThroughputDetails(self, details, expect_details):
        self.assert_consumed(details.capacity_unit, expect_details.capacity_unit)
        self.assert_time(details.last_increase_time, expect_details.last_increase_time)
        self.assert_time(details.last_decrease_time, expect_details.last_decrease_time)
        self.assert_equal(details.number_of_decreases_today, expect_details.number_of_decreases_today)

    def assert_UpdateTableResponse(self, response, expect_response):
        self.assert_ReservedThroughputDetails(response.reserved_throughput_details, expect_response.reserved_throughput_details)

    def assert_TableMeta(self, response, expect_response):
        self.assert_equal(response.table_name, expect_response.table_name)
        self.assert_equal(response.schema_of_primary_key, expect_response.schema_of_primary_key)

    def assert_DescribeTableResponse(self, response, expect_capacity_unit, expect_table_meta):
        self.assert_consumed(response.reserved_throughput_details.capacity_unit, expect_capacity_unit)
        self.assert_TableMeta(response.table_meta, expect_table_meta)

    def wait_for_capacity_unit_update(self, table_name):
        return global_test_lib.wait_for_partition_reload(ots2_api_test_config.instance_id_for_common, table_name)

    def wait_for_partition_load(self, table_name, instance_name=""):
        instance_id = ots2_api_test_config.instance_id_for_common
        if instance_name != "":
            instance_id = self.get_instance_id(instance_name)
        return global_test_lib.wait_for_partition_load(instance_id, table_name)

    def wait_for_CU_restore(self):
        time.sleep(restriction.CURestoreTimeInSec)

    def get_primary_keys(self, pk_cnt, pk_type, pk_name="PK", pk_value="x"):
        pk_schema = []
        pk = {}
        for i in range(pk_cnt):
            pk_schema.append(("%s%d" % (pk_name, i), pk_type))
            pk[("%s%d" % (pk_name, i))] = pk_value

        return pk_schema, pk

    def get_row_size(self, pk_dict, column_dict):
        sum = 0
        for k in pk_dict.keys():
            sum += len(k)
        for v in pk_dict.values():
            if isinstance(v, bool):
                sum += 1
            elif isinstance(v, (int, long)):
                sum += 8
            elif isinstance(v, (types.StringType, bytearray, unicode)):
                sum += len(v)
            else:
                raise Exception("wrong type is set in primary value")

        for k in column_dict.keys():
            sum += len(k)
        for v in column_dict.values():
            if v == None:
                pass
            elif isinstance(v, bool):
                sum += 1
            elif isinstance(v, (int, long, float)):
                sum += 8
            elif isinstance(v, (types.StringType, bytearray, unicode)):
                sum += len(v)
            else:
                raise Exception("wrong type is set in column value") 
        return sum

    def sum_CU_from_row(self, pk_dict, column_dict):
        sum = self.get_row_size(pk_dict, column_dict) 
        return int(math.ceil(sum * 1.0 / 4096))
    
    def set_DecreaseCapacityUnitDuration(self, duration):
        self.logger.debug("set global flag sqlol_master_DecreaseCapacityUnitDuration begin")
        cmd = '/apsara/deploy/rpc_caller --Server=nuwa://localcluster/sys/sqlonline-OTS/master --Method=/fuxi/SetGlobalFlag --Parameter="{\\"sqlol_master_DecreaseCapacityUnitPeriod\\": %d}"' % duration
        output = subprocess.check_output(cmd, shell=True)
        if "OK" not in output:
            raise Exception("set global flag sqlol_master_DecreaseCapacityUnitDuration fail")
        else:
            self.logger.debug("set global flag sqlol_master_DecreaseCapacityUnitDuration OK")

    def set_AdjustCapacityUnitInterval(self, interval):
        cmd = '/apsara/deploy/rpc_caller --Server=nuwa://localcluster/sys/sqlonline-OTS/master --Method=/fuxi/SetGlobalFlag --Parameter="{\\"sqlol_master_AdjustCapacityUnitInterval\\": %d}"' % interval
        if 'OK' != auto.run(cmd)[-2]:
            raise Exception("Failed to set global flag sqlol_master_AdjustCapacityUnitInterval")
    
    def _create_instance(self, instance_name_list, readcu=5000, writecu=5000):
        ots_client_list = []
        ocm_client = OCMClient(
                ots2_api_test_config.ocm_host,
                int(ots2_api_test_config.ocm_port),
                ots2_api_test_config.ocm_admin_access_id,
                ots2_api_test_config.ocm_admin_access_key
                )   
        raw_instance = ocm_client.get_instance(ots2_api_test_config.instance_name_for_common) 
        user_id = raw_instance['UserId']
        cluster_name = raw_instance['ClusterName']

        instance_list = [x['InstanceName'] for x in ocm_client.list_instance()]
        for inst_name in instance_name_list:
            self.logger.info("create or update instance with name %s", inst_name)
            if inst_name not in instance_list:
                ocm_client.insert_instance({
                    'InstanceName'    : inst_name,
                    'UserId'          : user_id,
                    'ClusterName'     : cluster_name,
                    'ReadCapacity'    : readcu,
                    'WriteCapacity'   : writecu,
                    'Quota'           : {}, 
                    'ModelType'       : 'BASIC',
                    'Description'     : '' 
                })  
            else:
                ocm_client.update_instance({
                    'InstanceName'    : inst_name,
                    'UserId'          : user_id,
                    'ReadCapacity'    : readcu,
                    'WriteCapacity'   : writecu,
                    'Quota'           : {}, 
                    'ModelType'       : 'BASIC',
                    'Description'     : '' 
                })
                instance_info = ocm_client.get_instance(inst_name)
                status = instance_info['Status']
                inst_id = instance_info['InstanceId']
                '''保证创建的instance的status为1'''
                if status == 2:
                    ocm_client.enable_instance(instance_id = inst_id)
                if status == 3:
                    ocm_client.delete_instance(inst_id)
                    ocm_client.insert_instance({
                        'InstanceName'    : inst_name,
                        'UserId'          : user_id,
                        'ClusterName'     : cluster_name,
                        'ReadCapacity'    : readcu,
                        'WriteCapacity'   : writecu,
                        'Quota'           : {}, 
                        'ModelType'       : 'BASIC',
                        'Description'     : '' 
                    })
            time.sleep(20)
            client = OTSClient(
                ots2_api_test_config.endpoint,
                ots2_api_test_config.access_id,
                ots2_api_test_config.access_key,
                inst_name,
                logger_name=atest.log.root.name,
            )
            for table_name in client.list_table():
                client.delete_table(table_name)
            ots_client_list.append(client)
        return ots_client_list

    def get_instance_id(self, instance_name):
        ocm_client = OCMClient(
                ots2_api_test_config.ocm_host,
                int(ots2_api_test_config.ocm_port),
                ots2_api_test_config.ocm_access_id,
                ots2_api_test_config.ocm_access_key
        )
        instance_info = ocm_client.get_instance(instance_name) 
        instance_id = instance_info['InstanceId']
        return instance_id
        
    def _create_table_with_4_pk(self, table_name):
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING'), 
            ('PK2', 'STRING'), ('PK3', 'STRING')])                
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit, 
            restriction.MaxReadWriteCapacityUnit
        ))
        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load(table_name)

    def _create_maxsize_row(self, pk_value = 'V'):
        """创建table+生成size恰好为max的pk dict和column dict"""
        pks = {}
        for i in range(0, 4):
            pk = 'PK' + str(i)
            value = pk_value * (restriction.MaxPKStringValueLength - len(pk))
            pks[pk] = value

        column_size = restriction.MaxPKStringValueLength * 4
        column_n = restriction.MaxColumnDataSizeForRow / column_size - 1
        columns = {}
        for i in range(0, column_n):
            key = 'C' + str(i)
            value = 'V' * (column_size - len(key))
            columns[key] = value
        return pks, columns

