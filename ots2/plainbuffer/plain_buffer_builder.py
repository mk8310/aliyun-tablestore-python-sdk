import sys
import ots2 
from ots2.metadata import *
from plain_buffer_consts import *
from plain_buffer_crc8 import *
from plain_buffer_stream import *
from plain_buffer_coded_stream import *

class PlainBufferBuilder:
    @staticmethod
    def compute_primary_key_value_size(value):
        size = 1   # TAG_CELL_VALUE
        size += const.LITTLE_ENDIAN_32_SIZE + 1  # length + type

        if (value is INF_MIN) or (value is INF_MAX) or (value is PK_AUTO_INCR):
            size += 1
            return size

        if isinstance(value, int) or isinstance(value, long):
            size += 8  #sizeof(int64_t)
        elif isinstance(value, str) or isinstance(value, unicode):
            size += const.LITTLE_ENDIAN_32_SIZE
            size += len(value)
        elif isinstance(value, bytearray):
            size += const.LITTLE_ENDIAN_32_SIZE
            size += len(value)
        else:
            raise OTSClientError("Unsupported primary key type:" + value_type)
        
        return size
       
    @staticmethod
    def compute_variant_value_size(value):
        return PlainBufferBuilder.compute_primary_key_value_size(value) - const.LITTLE_ENDIAN_32_SIZE - 1

    @staticmethod
    def compute_primary_key_column_size(pk_name, pk_value):
        size = 1
        size += 1 + const.LITTLE_ENDIAN_32_SIZE
        size += len(pk_name)
        size += PlainBufferBuilder.compute_primary_key_value_size(pk_value)
        size += 2
        return size
    
    @staticmethod
    def compute_column_value_size(value):
        size = 1
        size += const.LITTLE_ENDIAN_32_SIZE + 1
        
        if isinstance(value, int) or isinstance(value, long): 
            size += LITTLE_ENDIAN_64_SIZE
        elif isinstance(value, str) or isinstance(value, unicode):
            size += const.LITTLE_ENDIAN_32_SIZE
            size += len(value)
        elif isinstance(value, bytearray):
            size += const.LITTLE_ENDIAN_32_SIZE
            size += len(value)
        elif isinstance(value, bool):
            size += 1
        elif isinstance(value, float):
            size += LITTLE_ENDIAN_64_SIZE
        else:
            raise OTSClientError("Unsupported column type: " + str(type(value)))
        return size

    @staticmethod
    def compute_variant_value_size(column_value):
        return PlainBufferBuilder.compute_column_value_size(column_value) - const.LITTLE_ENDIAN_32_SIZE - 1
    
    @staticmethod
    def compute_column_size(column):
        size = 1
        size += 1 + const.LITTLE_ENDIAN_32_SIZE
        size += len(column.get_name())
        if column.value is not None:
            size += PlainBufferBuilder.compute_column_value_size(column.get_value())         
        if column.timestamp is not None:
            size += 1 + LITTLE_ENDIAN_64_SIZE
        size += 2
        return size
    
    @staticmethod
    def compute_column_size2(column, update_type):
        size = PlainBufferBuilder.compute_column_size(column)
        if update_type == DELETE or update_type == DELETE_ALL:
            size += 2
        return size
    
    @staticmethod
    def compute_primary_key_size(primary_key):
        size = 1
        for key in primary_key.keys():
            size += PlainBufferBuilder.compute_primary_key_column_size(key, primary_key.get(key))
        return size
    
    @staticmethod
    def compute_put_row_size(primary_key, attribute_columns):
        size = const.LITTLE_ENDIAN_32_SIZE
        size += PlainBufferBuilder.compute_primary_key_size(primary_key)

        if len(attribute_columns) != 0:
            size += 1
            for column in attribute_columns:
                size += PlainBufferBuilder.compute_column_size(column)
                
        size += 2
        return size
    
    @staticmethod
    def compute_update_row_size(row_change):
        size = const.LITTLE_ENDIAN_32_SIZE
        size += PlainBufferBuilder.compute_primary_key_size(rowChange.get_primary_key())

        columns = rowChange.get_columns()
        updateTypes = rowChange.get_update_types()

        if len(columns) != 0:
            size += 1
            for i in range(len(columns)):
                size += PlainBufferBuilder.compute_column_size(columns[i], updateTypes[i])
        size += 2
        return size
    
    @staticmethod
    def compute_delete_row_size(row_change):
        size = const.LITTLE_ENDIAN_32_SIZE
        size += PlainBufferBuilder.compute_primary_key_size(rowchange.get_primary_key())
        size += 3
        return size

    @staticmethod
    def serialize_primary_key_value(value):
        buf_size = PlainBufferBuilder.compute_variant_value_size(value)
        stream = PlainBufferOutputStream(buf_size)
        coded_stream = PlainBufferCodedOutputStream(stream)
    
        coded_stream.write_primary_key_value(value)
        return stream.get_buffer()

    @staticmethod
    def serialize_column_value(value):
        buf_size = PlainBufferBuilder.compute_variant_value_size(value)
        stream = PlainBufferOutputStream(buf_size)
        coded_stream = PlainBufferCodedOutputStream(stream)
    
        coded_stream.write_column_value(value)
        return stream.get_buffer()

    @staticmethod
    def serialize_primary_key(primary_key):
        buf_size = const.LITTLE_ENDIAN_32_SIZE
        buf_size += PlainBufferBuilder.compute_primary_key_size(primary_key)
        buf_size += 2

        output_stream = PlainBufferOutputStream(buf_size)
        coded_output_stream = PlainBufferCodedOutputStream(output_stream)
        
        row_checksum = 0
        coded_output_stream.write_header()

        row_checksum = coded_output_stream.write_primary_key(primary_key, row_checksum)
        row_checksum = PlainBufferCrc8.crc_int8(row_checksum, 0)
        coded_output_stream.write_row_checksum(row_checksum)
        return output_stream.get_buffer()

    @staticmethod
    def serialize_for_put_row(primary_key, attribute_columns):
        buf_size = PlainBufferBuilder.compute_put_row_size(primary_key, attribute_columns)
        output_stream = PlainBufferOutputStream(buf_size)
        coded_output_stream = PlainBufferCodedOutputStream(output_stream)

        row_checksum = 0
        coded_output_stream.write_header()
        row_checksum = coded_output_stream.write_primary_key(primary_key, row_checksum)
        row_checksum = coded_output_stream.write_columns(attribute_columns, row_checksum)
        row_checksum = PlainBufferCrc8.crc_int8(row_checksum, 0)
        coded_output_stream.write_row_checksum(row_checksum)
        return output_stream.get_buffer()

    @staticmethod
    def serialize_for_update_row(row_change):
        buf_size = PlainBufferBuilder.compute_row_size(row_change)
        output_stream = PlainBufferOutputStream(buf_size)
        coded_output_stream = PlainBufferCodedOutputStream(output_stream)

        row_checksum = 0
        coded_output_stream.Write_header()
        coded_output_stream.write_primary_key(rowChange.GetPrimaryKey(), row_checksum)
        coded_output_stream.write_columns(rowChange.get_columns(), row_change.get_update_types(), row_checksum)
        row_checksum = PlainBufferCrc8.crc_int8(row_checksum, 0)
        coded_output_stream.write_row_checksum(row_checksum)
        return output_stream.get_buffer()

    @staticmethod
    def serialize_for_delete_row(row_change):
        buf_size = PlainBufferBuilder.compute_row_size(row_change)
        output_stream = PlainBufferOutputStream(buf_size)
        coded_output_stream = PlainBufferCodedOutputStream(output_stream)

        row_checksum = 0
        coded_output_stream.write_header()
        coded_output_stream.Write_primary_key(rowChange.get_primary_key(), row_checksum)
        coded_output_stream.write_delete_marker(row_checksum)
        coded_output_stream.write_row_checksum(row_checksum)
        return output_stream.get_buffer()
