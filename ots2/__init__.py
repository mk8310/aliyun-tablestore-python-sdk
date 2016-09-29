# -*- coding: utf8 -*-

__version__ = '2.1.0'
__all__ = [
    'OTSClient',

    # Data Types
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
    'OTSClientError',
    'OTSServiceError',
    'DefaultRetryPolicy',
    'LogicalOperator',
    'ComparatorType',
    'ColumnConditionType',
    'ColumnCondition',
    'CompositeCondition',
    'RelationCondition',
    'RowExistenceExpectation',
]


from ots2.client import OTSClient

from ots2.metadata import *
from ots2.error import *
from ots2.retry import *

