# -*- coding: utf-8 -*-

from ots2_api_test_base import OTS2APITestBase
import restriction
from ots2 import *
import time
from ots2.error import *

class CapacityUnitCalcTest(OTS2APITestBase):

    """CapacityUnit逻辑测试"""
    def _try_to_consuming_max_CU(self,  table_name, pk_dict_exist, pk_dict_not_exist, capacity_unit):
        """此接口只为检查cu为（100*m， 100*n）"""
        read = capacity_unit.read
        write = capacity_unit.write
        columns = {}
        column_value_size = 4096
        all_pk_length = self.get_row_size(pk_dict_exist, {})
        #write
        for i in range(100):
            if i is not 0:
                columns['X' * i] = 'X' * (column_value_size - i)
            else:
                columns['col'] = 'X' * (column_value_size - all_pk_length - 10)
        consumed_update = self.client_test.put_row(table_name, Condition("IGNORE"), pk_dict_exist, columns)
        write_cu_expect = CapacityUnit(0, 100)
        self.assert_equal(100, self.sum_CU_from_row(pk_dict_exist, columns))
        self.assert_consumed(consumed_update, write_cu_expect)
        write = write - 100
        print write
        while write > 0:
            consumed_update = self.client_test.update_row(table_name, Condition("IGNORE"), pk_dict_exist, {'put':columns})
            self.assert_consumed(consumed_update, write_cu_expect)
            write = write - 100
            print write
        read_cu_expect = CapacityUnit(100, 0)
        while read > 0:
            consumed_read, primary_keys, columns_get_row = self.client_test.get_row(table_name, pk_dict_exist)
            self.assert_consumed(consumed_read, read_cu_expect)
            self.assert_equal(primary_keys, pk_dict_exist)
            self.assert_equal(columns_get_row, columns)
            read = read - 100
            print read
        """   考虑到回血，这里不需要进行强验证 
        #consume(0, 1)
        try:
            consumed_update = self.client_test.delete_row(table_name, Condition("IGNORE"), pk_dict_not_exist)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSNotEnoughCapacityUnit", "Remaining capacity unit is not enough.")
        #consume(1, 0)
        try:
            consumed_read, primary_keys, columns_get_row = self.client_test.get_row(table_name, pk_dict_not_exist)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSNotEnoughCapacityUnit", "Remaining capacity unit is not enough.")
        """
                                                                                                                        
    def wait_for_next_utc_day(self):
        now = int(time.time())
        next_day = now - now % (3600 * 24) + 3600 * 24
        if next_day - now <= 300:
            time.sleep(next_day - now + 5)

    def test_CU_raise_to_limit(self):
        """BUG#268910 BUG#268952 创建一个表，设置CU(1, 1)，每次上调(+1, +1)，一直上调到(max, max)，期望每次上调都成功，describe_table()确认CU为(max, max)，操作验证CU"""
        self.wait_for_next_utc_day()
        table_name = 'table_test_CU_rasie_to_limit'
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        pk_dict_exist = {'PK0':'a', 'PK1':1}
        pk_dict_not_exist = {'PK0':'B', 'PK1':2}
        reserved_throughput = ReservedThroughput(CapacityUnit(1, 1))

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test_CU_rasie_to_limit')

        count = 200
        expect_decrease_time = None
        while count <= restriction.MaxReadWriteCapacityUnit:
            time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
            expect_increase_time = int(time.time())
            reserved_throughput_new = ReservedThroughput(CapacityUnit(count, count))
            update_capacity_unit_response = self.client_test.update_table(table_name, reserved_throughput_new)
            expect_res = UpdateTableResponse(ReservedThroughputDetails(reserved_throughput_new.capacity_unit, expect_increase_time, expect_decrease_time, 0))
            self.assert_UpdateTableResponse(update_capacity_unit_response, expect_res)
            count = count + 200

        self.wait_for_capacity_unit_update('table_test_CU_rasie_to_limit')
        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput_new.capacity_unit, table_meta)

        self._try_to_consuming_max_CU(table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput_new.capacity_unit)

    def test_CU_raise_alternatively(self):
        """BUG#268910 BUG#268952 创建一个表，设置CU(1, 1)，交替每次上调(+200, 0)或者(0, +200)，一直上调到(max, max), 每次都describe_table()确认，操作验证CU"""
        self.wait_for_next_utc_day()
        table_name = 'table_test_CU_rasie_alternatively'
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        pk_dict_exist = {'PK0':'a', 'PK1':1}
        pk_dict_not_exist = {'PK0':'B', 'PK1':2}
        reserved_throughput = ReservedThroughput(CapacityUnit(1, 1))

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test_CU_rasie_alternatively')

        count = 200
        expect_decrease_time = None

        last_cu_write = 1
        while count <= restriction.MaxReadWriteCapacityUnit:
            time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
            expect_increase_time = int(time.time())
            reserved_throughput_new = ReservedThroughput(CapacityUnit(count, last_cu_write))
            update_capacity_unit_response = self.client_test.update_table(table_name, reserved_throughput_new)
            expect_res = UpdateTableResponse(ReservedThroughputDetails(reserved_throughput_new.capacity_unit, expect_increase_time, expect_decrease_time, 0))
            self.assert_UpdateTableResponse(update_capacity_unit_response, expect_res)
            describe_response = self.client_test.describe_table(table_name)
            self.assert_DescribeTableResponse(describe_response, reserved_throughput_new.capacity_unit, table_meta)

            time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
            expect_increase_time = int(time.time())
            reserved_throughput_new = ReservedThroughput(CapacityUnit(count, count))
            update_capacity_unit_response = self.client_test.update_table(table_name, reserved_throughput_new)
            expect_res = UpdateTableResponse(ReservedThroughputDetails(reserved_throughput_new.capacity_unit, expect_increase_time, expect_decrease_time, 0))
            self.assert_UpdateTableResponse(update_capacity_unit_response, expect_res)
            describe_response = self.client_test.describe_table(table_name)
            self.assert_DescribeTableResponse(describe_response, reserved_throughput_new.capacity_unit, table_meta)

            last_cu_write = count
            count += 200

        self.wait_for_capacity_unit_update('table_test_CU_rasie_alternatively')
        self._try_to_consuming_max_CU(table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput_new.capacity_unit)

    def test_CU_not_change_doesnot_count_as_reduce(self):
        """创建一个表，设置CU为(1, 1)，Update max + 1次CU(1, 1)，期望成功，describe_table()确认CU为(1, 1)，操作验证CU。"""
        self.wait_for_next_utc_day()
        table_name = 'table_test_CU_rasie_to_limit'
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(1, 1))
        pk_dict_exist = {'PK0':'a', 'PK1':1}
        pk_dict_not_exist = {'PK0':'B', 'PK1':2}

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test_CU_rasie_to_limit')

        count = 0 
        while count <= restriction.MaxCUReduceTimeLimit:
            time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
            expect_increase_time = int(time.time())
            update_capacity_unit_response = self.client_test.update_table(table_name, reserved_throughput)
            expect_decrease_time = None 
            expect_res = UpdateTableResponse(ReservedThroughputDetails(reserved_throughput.capacity_unit, expect_increase_time, expect_decrease_time, 0))
            self.assert_UpdateTableResponse(update_capacity_unit_response, expect_res)
            count = count + 1

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta)

        self.wait_for_capacity_unit_update('table_test_CU_rasie_to_limit')
        self.check_CU_by_consuming(table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput.capacity_unit)

                
    def test_CU_restore(self):
        """创建一个表，设置CU为(2, 2)，消耗为0后，经过CURestoreTimeInSec秒，验证CU恢复为(2, 2)；消耗为(1, 1)后，经过CURestoreTimeInSec / 2秒，验证CU恢复为(2, 2)"""
        self.wait_for_next_utc_day()
        table_name = 'table_test_CU_restore'
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        pk_dict_exist = {'PK0':'a', 'PK1':1}
        pk_dict_not_exist = {'PK0':'B', 'PK1':2} 
        pk_dict_exist_new = {'PK0':'C', 'PK1':2}
        reserved_throughput = ReservedThroughput(CapacityUnit(2, 2))

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test_CU_restore')
        #需不需要减去consuming的时间？？？
        self.check_CU_by_consuming(table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput.capacity_unit)
        time.sleep(restriction.CURestoreTimeInSec)
        self.check_CU_by_consuming(table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput.capacity_unit)
        time.sleep(restriction.CURestoreTimeInSec)
        reserved_throughput_new = ReservedThroughput(CapacityUnit(1, 1))
        self.try_to_consuming(table_name, pk_dict_exist_new,  pk_dict_not_exist, reserved_throughput_new.capacity_unit)
        time.sleep(restriction.CURestoreTimeInSec * 1.0 / 2)
        self.check_CU_by_consuming(table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput.capacity_unit)

    def test_CU_change_when_restore(self):
        """BUG#268910 BUG#268952 创建一个表，设置CU为(max, max)，消耗为0后，马上设置为(1, 1)，验证CU最终为(1, 1)，再消耗为0后，马上设置为(max, max)，验证CU最终为(max, max)"""
        self.wait_for_next_utc_day()
        table_name = 'table_test_CU_change_when_restore'
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        pk_dict_exist = {'PK0':'a', 'PK1':1}
        pk_dict_exist1 = {'PK0':'a', 'PK1':2}
        pk_dict_exist2 = {'PK0':'a', 'PK1':3}
        pk_dict_not_exist = {'PK0':'B', 'PK1':2}
        cu_count = restriction.MaxReadWriteCapacityUnit
        reserved_throughput = ReservedThroughput(CapacityUnit(cu_count, cu_count))
        reserved_throughput_new = ReservedThroughput(CapacityUnit(1, 1))

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('table_test_CU_change_when_restore')

        self._try_to_consuming_max_CU(table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput.capacity_unit)
        time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
        update_capacity_unit_response = self.client_test.update_table(table_name, reserved_throughput_new)
        self.wait_for_capacity_unit_update('table_test_CU_change_when_restore')

        time.sleep(restriction.CURestoreTimeInSec)
        self.check_CU_by_consuming(table_name, pk_dict_exist1,  pk_dict_not_exist, reserved_throughput_new.capacity_unit)

        time.sleep(restriction.AdjustCapacityUnitIntervalForTest)
        update_capacity_unit_response = self.client_test.update_table(table_name, reserved_throughput)
        self.wait_for_capacity_unit_update('table_test_CU_change_when_restore')

        time.sleep(restriction.CURestoreTimeInSec)
        self._try_to_consuming_max_CU(table_name, pk_dict_exist2,  pk_dict_not_exist, reserved_throughput.capacity_unit)


    def test_create_table_with_huge_cu(self):
        '''BUG#273584 绯荤粺鏀寔瀵瑰崟partition涓奀U鏈夎缃笂闄愶紝閬垮厤鍗昿artition CU璁剧疆杩囧ぇ锛屽鑷村崟鏈鸿鍘嬪灝'''
        
        self.wait_for_next_utc_day()
        ots_client = self._create_instance(['hugeCU-0'], 20000, 20000)[0]

        table_name = 'test_create_table_with_huge_cu'
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER')])
        
        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit + 1, restriction.MaxReadWriteCapacityUnit + 1))
        try:
            ots_client.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")

        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit + 1, restriction.MaxReadWriteCapacityUnit))
        try:
            ots_client.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")

        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit + 1))
        try:
            ots_client.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")

        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
        ots_client.create_table(table_meta, reserved_throughput)

    def test_update_table_with_huge_cu_in_single_partition(self):
        '''BUG#273584 绯荤粺鏀寔瀵瑰崟partition涓奀U鏈夎缃笂闄愶紝閬垮厤鍗昿artition CU璁剧疆杩囧ぇ锛屽鑷村崟鏈鸿鍘嬪灝'''
        
        self.wait_for_next_utc_day()
        ots_client = self._create_instance(['hugeCU-1'], 20000, 20000)[0]
         
        table_name = 'test_update_table_with_huge_cu_in_single_partition'
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER')])

        reserved_throughput = ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit, restriction.MaxReadWriteCapacityUnit))
        ots_client.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load('test_update_table_with_huge_cu_in_single_partition')
        
        time.sleep(10)

        try:
            ots_client.update_table(table_name, ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit + 1, None)))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")
        time.sleep(10)
        try:
            ots_client.update_table(table_name, ReservedThroughput(CapacityUnit(None, restriction.MaxReadWriteCapacityUnit + 1)))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")
        time.sleep(10)
        try:
            ots_client.update_table(table_name, ReservedThroughput(CapacityUnit(restriction.MaxReadWriteCapacityUnit + 1, restriction.MaxReadWriteCapacityUnit + 1)))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSTooManyCapacityUnits", "This table can not serve so many capacity units.")

    def test_getrow_consume_cu(self):
        """
    http://k3.alibaba-inc.com/issue/6452586
    测试GetRow在后端确实消耗了CU
    1. 建表，读CU为10
    2. 写一行数据，100CU
    3. 读这行数据，消耗100CU
    4. 再读同一行，预计收到CU不足的错误
        """
        table_name = 'table_test_getrow_consume_cu'
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(10, 100))

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pk = {'PK0':'a', 'PK1':1}
        columns = {}
        for i in range(10):
            columns['col' + str(i)] = 'x' * 10 * 4096

        self.client_test.put_row(table_name, Condition("IGNORE"), pk, columns)

        consumed_read, rpks, rcols = self.client_test.get_row(table_name, pk)

        read_cu = self.sum_CU_from_row(pk, columns)
        read_cu_expect = CapacityUnit(read_cu, 0)
        self.assert_consumed(read_cu_expect, consumed_read)

        # get row again should get OTSNotEnoughCapacityUnit
        try:
            self.client_test.get_row(table_name, pk)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSNotEnoughCapacityUnit", "Remaining capacity unit for read is not enough.")

    def test_putrow_consume_cu(self):
        """
    http://k3.alibaba-inc.com/issue/6452586
    测试ModifyRow在后端确实消耗了CU
    1. 建表，写CU为10
    2. 写一行数据，消耗100CU
    3. 再写同一行，预计收到CU不足的错误
        """
        table_name = 'table_test_putrow_consume_cu'
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(1, 10))

        self.client_test.create_table(table_meta, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pk = {'PK0':'a', 'PK1':1}
        columns = {}
        for i in range(10):
            columns['col' + str(i)] = 'x' * 10 * 4096

        consumed_update = self.client_test.put_row(table_name, Condition("IGNORE"), pk, columns)
        write_cu = self.sum_CU_from_row(pk, columns)
        write_cu_expect = CapacityUnit(0, write_cu)
        self.assert_consumed(write_cu_expect, consumed_update)

        # put row again should get OTSNotEnoughCapacityUnit
        try:
            consumed_update = self.client_test.put_row(table_name, Condition("IGNORE"), pk, columns)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSNotEnoughCapacityUnit", "Remaining capacity unit for write is not enough.")
