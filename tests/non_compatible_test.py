# -*- coding: utf8 -*-

#ots2
import restriction
import ots2_api_test_config
from ots2_api_test_base import OTS2APITestBase
from ots2 import *
from ots2.error import *
import atest.log
#ots1
import sys
from ots.client import OTSClient as OTSClient1
from ots.metadata import TableMeta as TableMeta1
from ots.error import OTSAuthFailed 

#ocm 
from ocm_client import *


class NonCompatibleTest(OTS2APITestBase):

    """1.0 API与2.0 API非兼容测试"""
    def _create_instance1_0(self, inst_name):
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
        if inst_name not in instance_list:
            ocm_client.insert_instance({
            'InstanceName'    : inst_name,
            'ClusterName'     : cluster_name,
            'UserId'          : user_id,
            'ReadCapacity'    : 200,
            'WriteCapacity'   : 200,
            'Quota'           : { },  
            'ModelType'       : 'LEGACY',
            'Description'     : '', 
            })  
        else:
             ocm_client.update_instance({
            'InstanceName'    : inst_name,
            'UserId'          : user_id,
            'ReadCapacity'    : 200,
            'WriteCapacity'   : 200,
            'Quota'           : { },  
            'ModelType'       : 'LEGACY',
            'Description'     : '', 
        })  
        time.sleep(20)
        client = OTSClient1(
        ots2_api_test_config.endpoint,
        ots2_api_test_config.access_id,
        ots2_api_test_config.access_key,
        inst_name,
        )   
        for table_name in client.list_table(): 
            client.delete_table(table_name)
     
    def test_api2_with_sdk1(self):
        """用1.0 API的SDK访问2.0 API，期望被拒绝"""
        self._create_instance1_0("inst10")
        client_ots1 = OTSClient(
                ots2_api_test_config.endpoint,
                ots2_api_test_config.access_id,
                ots2_api_test_config.access_key,
                "inst10",
                logger_name=atest.log.root.name,
                )
        
        table_name = "table_test"
        table_meta = TableMeta(table_name, [("PK", "STRING")])
        reserved_throughput = ReservedThroughput(CapacityUnit(50, 50))
        try:
            client_ots1.create_table(table_meta, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSAuthFailed", "The instance's mode type is LEGACY but your SDK's model type is PUBLIC.")
       
    def test_api1_with_sdk2(self):
        """用2.0 API的SDK访问1.0 API，期望被拒绝"""
        client_ots2 = OTSClient1(
                ots2_api_test_config.endpoint,
                ots2_api_test_config.access_id,
                ots2_api_test_config.access_key,
                ots2_api_test_config.instance_name_for_common
                )
        table_name = "table_test"
        table_meta = TableMeta1(table_name, [("PK", "STRING")])
        try:
            client_ots2.create_table(table_meta)
            self.assert_false()
        except OTSAuthFailed as e:
            self.assertEqual(e.code, "OTSAuthFailed")
            self.assertEqual(e.message, "The instance's mode type is PUBLIC but your SDK's model type is LEGACY.")

