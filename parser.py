import struct
import logging

class PacketParser:

    @staticmethod
    def byte_encode_ok_packet(packet_string):
        result = None
        parts = packet_string.strip().split(' ')
        try:
            node_id = parts[1]
            node_type = parts[2]
        except IndexError:
            logging.error("PRS_ERROR: Missing ID and/or type, " + packet_string)
            return result

        packed = b''

        try:
            for s in parts[3:]:
                packed += struct.pack('B', int(s))
            result = (node_id, node_type, packed)
        except struct.error:
            logging.error("PRS_ERROR: Cannot pack, " + repr(parts))
        finally:
            return result 

    @staticmethod
    def unpack_packed_bytes(format, packed_string):
        data = None
        try:
            data = struct.unpack(format, packed_string)
        except struct.error:
            logging.error("PRS_ERROR: Unable to unpack, " + packed_string)
        finally:
            return data
