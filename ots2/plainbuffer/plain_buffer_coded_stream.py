import sys
import struct
from ots2.const import *
from ots2.metadata import *
from ots2.error import *
from plain_buffer_consts import *
from plain_buffer_crc8 import *

class PlainBufferCodedInputStream:
    def __init__(self, inputStream):
        self.inputStream = inputStream

    def read_tag(self):
        return self.inputStream.read_tag()

    def check_last_tag_was(self, tag):
        return self.inputStream.check_last_tag_was(tag)

    def get_last_tag(self):
        return self.inputStream.get_last_tag()

    def read_header(self):
        return self.inputStream.read_int32()

    def read_primary_key_value(self, cell_check_sum):
        if not self.check_last_tag_was(TAG_CELL_VALUE):
            raise OTSClientError("Expect TAG_CELL_VALUE but it was " + self.get_last_tag())

        self.inputStream.read_raw_little_endian32()
        column_type = ord(self.inputStream.read_raw_byte())
        if column_type == VT_INTEGER:
            int64_value = self.inputStream.read_int64()
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, int64_value)
            self.read_tag()
            return (int64_value, cell_check_sum)
        elif column_type == VT_STRING:
            value_size = self.inputStream.read_int32()
            string_value = self.inputStream.read_utf_string(value_size)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, string_value)
            self.read_tag()
            return (string_value, cell_check_sum)
        elif column_type == VT_BLOB:
            value_size = self.inputStream.read_int32() 
            binary_value = self.inputStream.read_bytes(value_size)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, binary_value)            
            self.read_tag()
            return (binary_value, cell_check_sum)
        else:
            raise OTSClientError("Unsupported primary key type: " + column_type)

    def read_column_value(self, cell_check_sum):
        if not self.check_last_tag_was(TAG_CELL_VALUE):
            raise OTSClientError("Expect TAG_CELL_VALUE but it was" + self.get_last_tag())
        self.inputStream.read_raw_little_endian32()
        column_type = ord(self.inputStream.read_raw_byte())
        if column_type == VT_INTEGER:
            int64_value = self.inputStream.read_int64()
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, int64_value)
            self.read_tag()
            return (int64_value, cell_check_sum)
        elif column_type == VT_STRING:
            value_size = self.inputStream.read_int32()
            string_value = self.inputStream.read_utf_string(value_size)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, string_value)
            self.read_tag()
            return (string_value, cell_check_sum)
        elif column_type == VT_BLOB:
            value_size = self.inputStream.read_int32() 
            binary_value = self.inputStream.read_bytes(value_size)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, binary_value)            
            self.read_tag()
            return (binary_value, cell_check_sum)
        elif column_type == VT_BOOLEAN:
            bool_value = self.inputStream.read_boolean()
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BOOLEAN)
            bool_int8 = 1
            if bool_value is None:
                bool_int8 = 0
            #bool_int8 = bool_value != null ? 1 : 0
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, bool_int8)
            self.read_tag()
            return (bool_value, cell_check_sum)
        elif column_type == VT_DOUBLE:
            double_int = self.inputStream.read_double()
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_DOUBLE)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, double_int)
            self.read_tag()
            double_value, = struct.unpack('d', struct.pack('l', double_int))
            return (double_value, cell_check_sum)
        else:
            raise OTSClientError("Unsupported column type: " + column_type)

    def read_primary_key_column(self, row_check_sum):
        if not self.check_last_tag_was(TAG_CELL):
            raise OTSClientError("Expect TAG_CELL but it was " + self.get_last_tag())
        self.read_tag()

        if not self.check_last_tag_was(TAG_CELL_NAME):
            raise "Expect TAG_CELL_NAME but it was " + self.get_last_tag()
        
        cell_check_sum = 0
        name_size = self.inputStream.read_raw_little_endian32()
        column_name = self.inputStream.read_utf_string(name_size)
        cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, column_name)
        self.read_tag()
        
        if not self.check_last_tag_was(TAG_CELL_VALUE):
            raise OTSClientError("Expect TAG_CELL_VALUE but it was " + self.get_last_tag())
        
        primary_key_value, cell_check_sum = self.read_primary_key_value(cell_check_sum)
        
        if self.get_last_tag() == TAG_CELL_CHECKSUM:
            check_sum = ord(self.inputStream.read_raw_byte())
            if check_sum != cell_check_sum:
                raise OTSClientError("Checksum mismatch. expected:" + str(check_sum) + ",actual:" + str(cell_check_sum))
            self.read_tag()
        else:
            raise OTSClientError("Expect TAG_CELL_CHECKSUM but it was " + str(self.get_last_tag()))
        
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return (column_name, primary_key_value, row_check_sum)

    def read_column(self, row_check_sum):
        if not self.check_last_tag_was(TAG_CELL):
            raise OTSClientError("Expect TAG_CELL but it was " + str(self.get_last_tag()))
        self.read_tag()

        if not self.check_last_tag_was(TAG_CELL_NAME):
            raise OTSClientError("Expect TAG_CELL_NAME but it was " + str(self.get_last_tag()))
                   
        cell_check_sum = 0
        name_size = self.inputStream.read_raw_little_endian32()
        column_name = self.inputStream.read_utf_string(name_size)
        cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, column_name)
        self.read_tag()
    
        column = Column(column_name)
        
        if self.get_last_tag() == TAG_CELL_VALUE:
            column.value, cell_check_sum = self.read_column_value(cell_check_sum)
        # skip CELL_TYPE
        if self.get_last_tag() == TAG_CELL_TYPE:
            cell_type = PlainBufferCrc8.crc_int8(cell_check_sum, cell_type)
            self.read_tag()
        
        if self.get_last_tag() == TAG_CELL_TIMESTAMP:
            timestamp = self.inputStream.read_int64()
            column.timestamp = timestamp
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, timestamp)
            self.read_tag()

        if self.get_last_tag() == TAG_CELL_CHECKSUM:
            check_sum = ord(self.inputStream.read_raw_byte())
            if check_sum != cell_check_sum:
                raise OTSClientError("Checksum mismatch.")
            self.read_tag()
        else:
            raise OTSClientError("Expect TAG_CELL_CHECKSUM but it was " + str(self.get_last_tag()))
        
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return column, row_check_sum

    def read_row_without_header(self):
        row_check_sum = 0
        primary_key = {}
        attribute_columns = []
        
        if not self.check_last_tag_was(TAG_ROW_PK):
            raise OTSClientError("Expect TAG_ROW_PK but it was " + str(self.get_last_tag()))

        self.read_tag()
        
        while self.check_last_tag_was(TAG_CELL):
            (name, value, row_check_sum) = self.read_primary_key_column(row_check_sum)
            primary_key[name] = value

        if self.check_last_tag_was(TAG_ROW_DATA):
            self.read_tag()
            while self.check_last_tag_was(TAG_CELL):
                column, row_check_sum = self.read_column(row_check_sum)
                attribute_columns.append(column)

        if self.check_last_tag_was(TAG_DELETE_ROW_MARKER):
            self.read_tag()
            row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, 1)
        else:
            row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, 0)

        if self.check_last_tag_was(TAG_ROW_CHECKSUM):
            check_sum = ord(self.inputStream.read_raw_byte())
            if check_sum != row_check_sum:
                raise OTSClientError("Checksum is mismatch.")
            self.read_tag()
        else:
            raise OTSClientError("Expect TAG_ROW_CHECKSUM but it was " + str(self.get_last_tag()))
        
        return primary_key, attribute_columns

    def read_row(self):
        if self.read_header() != HEADER:
            raise OTSClientError("Invalid header from plain buffer.")
        self.read_tag()
        return self.read_row_without_header()

class PlainBufferCodedOutputStream:
    def __init__(self, outputStream):
        self.outputStream = outputStream

    def write_header(self):
        self.outputStream.write_raw_little_endian32(HEADER)

    def write_tag(self, tag):
        self.outputStream.write_raw_byte(tag)

    def write_cell_name(self, name, cell_check_sum):
        self.write_tag(TAG_CELL_NAME)
        self.outputStream.write_raw_little_endian32(len(name))
        self.outputStream.write_bytes(name)
        cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, name)
        return cell_check_sum

    def write_primary_key_value(self, value, cell_check_sum):
        self.write_tag(TAG_CELL_VALUE)
        if value is INF_MIN:
            self.outputStream.write_raw_little_endian32(1)
            self.outputStream.write_raw_byte(VT_INF_MIN)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INF_MIN)
        elif value is INF_MAX:
            self.outputStream.write_raw_little_endian32(1)
            self.outputStream.write_raw_byte(VT_INF_MAX)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INF_MAX)
        elif isinstance(value, int) or isinstance(value, long):
            self.outputStream.write_raw_little_endian32(1 + const.LITTLE_ENDIAN_64_SIZE)
            self.outputStream.write_raw_byte(VT_INTEGER)
            self.outputStream.write_raw_little_endian64(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, value)
        elif isinstance(value, str) or isinstance(value, unicode):
            string_value = value
            prefix_length = const.LITTLE_ENDIAN_32_SIZE + 1
            self.outputStream.write_raw_little_endian32(prefix_length + len(string_value))
            self.outputStream.write_raw_byte(VT_STRING)
            self.outputStream.write_raw_little_endian32(len(string_value))
            self.outputStream.write_bytes(string_value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, len(string_value))
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, string_value)
        elif isinstance(value, bytearray):
            binary_value = value
            prefix_length = const.LITTLE_ENDIAN_32_SIZE + 1

            self.outputStream.write_raw_little_endian32(prefix_length + len(binary_value))
            self.outputStream.write_raw_byte(VT_BLOB)
            self.outputStream.write_raw_little_endian32(len(binary_value))
            self.outputStream.write_bytes(binary_value)

            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, len(binary_value))
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, binary_value) 
        else:
            raise OTSClientError("Unsupported primary key type: " + type(value)) 
        return cell_check_sum

    def write_column_value_with_checksum(self, value, cell_check_sum):
        self.write_tag(TAG_CELL_VALUE)
        if isinstance(value, int) or isinstance(value, long):
            self.outputStream.write_raw_little_endian32(1 + LITTLE_ENDIAN_64_SIZE)
            self.outputStream.write_raw_byte(VT_INTEGER)
            self.outputStream.write_raw_little_endian64(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, value)
        elif isinstance(value, str) or isinstance(value, unicode):
            prefix_length = LITTLE_ENDIAN_32_SIZE + 1 
            self.outputStream.write_raw_little_endian32(prefix_length + len(value)) 
            self.outputStream.write_raw_byte(VT_STRING)
            self.outputStream.write_raw_little_endian32(len(value))
            self.outputStream.write_bytes(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, len(value))
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, value)
        elif isinstance(value, bytearray):
            prefix_length = LITTLE_ENDIAN_32_SIZE + 1
            self.outputStream.write_raw_little_endian32(prefix_length + len(value))
            self.outputStream.write_raw_byte(VT_BLOB)
            self.outputStream.write_raw_little_endian32(len(value))
            self.outputStream.write_bytes(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, len(value))
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, value)
        elif isinstance(value, bool):
            self.outputStream.write_raw_little_endian32(2)
            self.outputStream.write_raw_byte(VT_BOOLEAN)
            self.outputStream.write_boolean(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BOOLEAN)
            if value:
                cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, 1)
            else:
                cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, 0)
        elif isinstance(value, float):
            double_in_long, = struct.unpack("l", struct.pack("d", value))
            self.outputStream.write_raw_little_endian32(1 + LITTLE_ENDIAN_64_SIZE)
            self.outputStream.write_raw_byte(VT_DOUBLE)
            self.outputStream.write_double(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_DOUBLE)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, double_in_long)
        else:
            raise OTSClientError("Unsupported column type: " + type(value))
        return cell_check_sum

    def write_column_value(self, value):
        if isinstance(value, int) or isinstance(value, long):
            self.outputStream.write_raw_byte(VT_INTEGER)
            self.outputStream.write_raw_little_endian64(value)
        elif isinstance(value, str) or isinstance(value, unicode):
            self.outputStream.write_raw_byte(VT_STRING)
            self.outputStream.write_raw_little_endian32(len(value))
            self.outputStream.write_bytes(value)
        elif isinstance(value, bytearray):
            self.outputStream.write_raw_byte(VT_BLOB)
            self.outputStream.write_raw_little_endian32(len(value))
            self.outputStream.write_bytes(value)
        elif isinstance(value, bool):
            self.outputStream.write_raw_byte(VT_BOOLEAN)
            self.outputStream.write_boolean(value)
        elif isinstance(value, float):  
            self.outputStream.write_raw_byte(VT_DOUBLE)
            self.outputStream.write_double(value)
        else:
            raise OTSClientError("Unsupported column type: " + str(value.GetType()))

    def write_primary_key_column(self, pk_name, pk_value, row_check_sum):
        cell_check_sum = 0
        self.write_tag(TAG_CELL)
        cell_check_sum = self.write_cell_name(pk_name, cell_check_sum)
        cell_check_sum = self.write_primary_key_value(pk_value, cell_check_sum)
        self.write_tag(TAG_CELL_CHECKSUM)
        self.outputStream.write_raw_byte(cell_check_sum)
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return row_check_sum

    def write_column(self, column, row_check_sum):
        cell_check_sum = 0
        self.write_tag(TAG_CELL)
        cell_check_sum = self.write_cell_name(column.name, cell_check_sum)
        cell_check_sum = self.write_column_value_with_checksum(column.value, cell_check_sum)
        if column.timestamp is not None:
            self.write_tag(TAG_CELL_TIMESTAMP)
            self.outputStream.write_raw_little_endian64(column.timestamp)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, column.timestamp)
        self.write_tag(TAG_CELL_CHECKSUM)
        self.outputStream.write_raw_byte(cell_check_sum)
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return row_check_sum

    def write_column2(self, column, column_type, row_check_sum):
        pass

    def write_primary_key(self, primary_key, row_check_sum):
        self.write_tag(TAG_ROW_PK)
        for pk_name in primary_key.keys():
            row_check_sum = self.write_primary_key_column(pk_name, primary_key.get(pk_name), row_check_sum)
        return row_check_sum

    def write_columns(self, columns, row_check_sum):
        if columns is not None and len(columns) != 0:
            self.write_tag(TAG_ROW_DATA)
            for column in columns:
                row_check_sum = self.write_column(column, row_check_sum)
        return row_check_sum

    def write_columns2(self, columns, update_types, row_check_sum):
        pass

    def write_delete_marker(self, row_check_sum):
        pass

    def write_row_checksum(self, row_check_sum):
        self.write_tag(TAG_ROW_CHECKSUM)                                                                                  
        self.outputStream.write_raw_byte(row_check_sum)
