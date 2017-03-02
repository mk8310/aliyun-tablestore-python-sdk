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
    def compute_column_size(column_name, column_value):
        size = 1
        size += 1 + const.LITTLE_ENDIAN_32_SIZE
        size += len(column_name)
        timestamp = None
        if column_value is not None:
            if isinstance(column_value, tuple):
                if column_value[0] is not None:
                    size += PlainBufferBuilder.compute_column_value_size(column_value[0]) 
                if column_value[1] is not None:
                    timestamp = column_value[1]
            else:
                size += PlainBufferBuilder.compute_column_value_size(column_value) 
        if timestamp is not None:
            size += 1 + LITTLE_ENDIAN_64_SIZE
        size += 2
        return size
    
    @staticmethod
    def compute_column_size2(column_name, column_value, update_type):
        size = PlainBufferBuilder.compute_column_size(column_name, column_value)
        if update_type == UpdateType.DELETE or update_type == UpdateType.DELETE_ALL:
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
            for column_name in attribute_columns.keys():
                size += PlainBufferBuilder.compute_column_size(column_name, attribute_columns[column_name])
                
        size += 2
        return size

    @staticmethod
    def compute_update_row_size(primary_key, attribute_columns):
        size = const.LITTLE_ENDIAN_32_SIZE
        size += PlainBufferBuilder.compute_primary_key_size(primary_key)

        if len(attribute_columns) != 0:
            size += 1
            for update_type in attribute_columns.keys():
                columns = attribute_columns[update_type]
                if isinstance(columns, dict):
                    for column_name in columns.keys():
                        size += PlainBufferBuilder.compute_column_size2(column_name, columns[column_name], update_type)
                elif isinstance(columns, list):
                    for column_name in columns:
                        size += PlainBufferBuilder.compute_column_size2(column_name, None, update_type)
                else:
                    raise OTSClientError("Unsupported column type:" + str(type(columns)))
                
        size += 2
        return size
    
    @staticmethod
    def compute_delete_row_size(primary_key):
        size = const.LITTLE_ENDIAN_32_SIZE
        size += PlainBufferBuilder.compute_primary_key_size(primary_key)
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
    def serialize_for_update_row(primary_key, attribute_columns):
        buf_size = PlainBufferBuilder.compute_update_row_size(primary_key, attribute_columns)
        output_stream = PlainBufferOutputStream(buf_size)
        coded_output_stream = PlainBufferCodedOutputStream(output_stream)

        row_checksum = 0
        coded_output_stream.write_header()
        row_checksum = coded_output_stream.write_primary_key(primary_key, row_checksum)
        row_checksum = coded_output_stream.write_update_columns(attribute_columns, row_checksum)
        row_checksum = PlainBufferCrc8.crc_int8(row_checksum, 0)
        coded_output_stream.write_row_checksum(row_checksum)
        return output_stream.get_buffer()

    @staticmethod
    def serialize_for_delete_row(primary_key):
        buf_size = PlainBufferBuilder.compute_delete_row_size(primary_key)
        output_stream = PlainBufferOutputStream(buf_size)
        coded_output_stream = PlainBufferCodedOutputStream(output_stream)

        row_checksum = 0
        coded_output_stream.write_header()
        row_checksum = coded_output_stream.write_primary_key(primary_key, row_checksum)
        row_checksum = coded_output_stream.write_delete_marker(row_checksum)
        coded_output_stream.write_row_checksum(row_checksum)
        return output_stream.get_buffer()
