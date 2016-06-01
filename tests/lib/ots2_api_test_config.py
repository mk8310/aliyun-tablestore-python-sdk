import global_test_config

endpoint = 'http://10.101.160.179:9981'

access_id = 'OTSMultiUser100_accessid'
access_key = 'OTSMultiUser100_accesskey'
instance_name_for_common = 'BasicA00'
instance_id_for_common = 'BasicA00ID'

instance_name_for_user_privilege_test_0 = 'Privilege0'
instance_name_for_user_privilege_test_1 = 'Privilege1'
user_id_for_user_privilege_test = '108150108102'
access_id_for_user_privilege_test = 'OTSMultiUser102_accessid'
access_key_for_user_privilege_test = 'OTSMultiUser102_accesskey'
ots_servers = [
    '10.101.160.179:9997',
    '10.101.160.183:9997',
    '10.101.160.181:9997',
    '10.101.160.190:9997',
    '10.101.160.191:9997',
    '10.101.160.174:9997',
]

ocm_endpoint = '10.101.160.179:9980'
host, port = ocm_endpoint.split(":")
ocm_port = port
ocm_host = host
ocm_access_id = "test_accessid"
ocm_access_key = "test_accesskey"
ocm_admin_access_id = "admin_accessid"
ocm_admin_access_key = "admin_accesskey"

sqlconsole = global_test_config.sqlonline_console
database_name = "OTSPublicDB"
service_name = "sqlonline-OTS"


another_cluster_access_id = 'OTSMultiUser101_accessid'
another_cluster_access_key = 'OTSMultiUser101_accesskey'
another_cluster_instance_name = 'BasicB00'
another_cluster_instance_id = 'BasicB00ID'

umm_endpoint = 'umm-test.aliyun-inc.com:8400'
umm_access_id = 'bqzqdficcfq5ub6b6qzox04p'
umm_access_key = '54WyhDGgULRXJKbExgEhe2RXnu4='
