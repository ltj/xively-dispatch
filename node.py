import xively
import requests
import logging
from parser import PacketParser
from config import DEBUG

class Node():

    @classmethod
    def updateDataStreams(cls, id, packed):
        data = PacketParser.unpack_packed_bytes(cls.format, packed)
        result = []
        if data != None:
            for d in cls.datastreams:
                if 'transform' in d:
                    cval = d['transform'](data[d['dataindex']])
                else:
                    cval = data[d['dataindex']]
                result.append(xively.Datastream(id=id + d['suffix'], current_value=cval))
        return result

    @classmethod
    def createDataStreams(cls, feed, id):
        for d in cls.datastreams:
            feed.datastreams.create(id=id + d['suffix'], tags=d['tags'], unit=d['unit'])



class RoomNode(Node):

    format = '<hhHB'
    datastreams = [{'suffix': '_temperature', 'dataindex': 0, 'tags': 'temperature', 
                    'unit': xively.Unit(label='celsius', symbol='℃'), 
                    'transform': lambda x: round(x/10.0, 1)},
                   {'suffix': '_humidity', 'dataindex': 1, 'tags': 'humidity',
                    'unit': xively.Unit(label='rh', symbol='%')},
                   {'suffix': '_light', 'dataindex': 2, 'tags': 'light',
                    'unit': xively.Unit(label='lux', symbol='lx')},
                   {'suffix': '_voltage', 'dataindex': 3, 'tags': 'voltage',
                    'unit': xively.Unit(label='volt', symbol='V'),
                    'transform': lambda x: round(x*0.02+1, 2)}]


class WeatherNode(Node):

    format = '<hhiH'
    datastreams = [{'suffix': '_indoor_temperature', 'dataindex': 0, 'tags': 'temperature', 
                    'unit': xively.Unit(label='celsius', symbol='℃'), 
                    'transform': lambda x: round(x/10.0, 1)},
                   {'suffix': '_temperature', 'dataindex': 1, 'tags': 'temperature', 
                    'unit': xively.Unit(label='celsius', symbol='℃'), 
                    'transform': lambda x: round(x/16.0, 1)},
                   {'suffix': '_pressure', 'dataindex': 2, 'tags': 'pressure',
                    'unit': xively.Unit(label='pascal', symbol='hPa'),
                    'transform': lambda x: round(x/100.0, 1)},
                   {'suffix': '_voltage', 'dataindex': 3, 'tags': 'voltage',
                    'unit': xively.Unit(label='volt', symbol='V'),
                    'transform': lambda x: round(x/1000, 2)}]


class PowerNode(Node):

    format = '<hfB'
    datastreams = [{'suffix': '_power', 'dataindex': 0, 'tags': 'power', 
                    'unit': xively.Unit(label='watts', symbol='W')},
                   {'suffix': '_kwh', 'dataindex': 1, 'tags': 'power',
                    'unit': xively.Unit(label='watt hours', symbol='kWh'),
                    'transform': lambda x: round(x, 3)}]

class TempMicroNode(Node):

    format = '<hBB'
    datastreams = [{'suffix': '_temperature', 'dataindex': 0, 'tags': 'temperature', 
                    'unit': xively.Unit(label='celsius', symbol='℃'), 
                    'transform': lambda x: round(x/16.0, 1)},
                   {'suffix': '_pre_voltage', 'dataindex': 1, 'tags': 'voltage',
                    'unit': xively.Unit(label='volt', symbol='V'),
                    'transform': lambda x: round(x*0.02+1, 2)},
                   {'suffix': '_post_voltage', 'dataindex': 2, 'tags': 'voltage',
                    'unit': xively.Unit(label='volt', symbol='V'),
                    'transform': lambda x: round(x*0.02+1, 2)}]

class DispatchFactory:

    def __init__(self, API_KEY, FEED_ID):
        self.api = xively.XivelyAPIClient(API_KEY)
        self.feed = self.api.feeds.get(FEED_ID)
        self.knownIDs = set()
        self.__populateKnownIDs()
        if DEBUG:
            logging.debug("XIV: Initialized DispatchFactory")
            logging.debug("XIV_KNOWNIDS: " + repr(self.knownIDs))

    def __populateKnownIDs(self):
        for d in self.feed.datastreams:
            id = int(d.id.split('_')[0])
            self.knownIDs.add(id)

    @classmethod
    def getNode(cls, type):
        if type == 0:
            return RoomNode
        elif type == 1:
            return WeatherNode
        elif type == 2:
            return PowerNode
        elif type == 3:
            return TempMicroNode
        else:
            return None

    def handleNewPacket(self, packet):
        parse_result = PacketParser.byte_encode_ok_packet(packet)
        if parse_result != None:
            (id, type, packed) = parse_result
            node = self.getNode(int(type))
            if DEBUG:
                logging.debug("NODE: id {0}, type {1}".format(id, type))

            if int(id) not in self.knownIDs:
                node.createDataStreams(self.feed, id)
                self.knownIDs.add(int(id))

            if node != None:
                update = node.updateDataStreams(id, packed)

                if update != []:
                    self.feed.datastreams = update
                    try:
                        self.feed.update(fields=['datastreams'])
                        if DEBUG:
                            logging.debug("DSTRM_UPDATED:" + id + ", " + type + ", " + repr(update))
                    except requests.ConnectionError as e:
                        logging.error("XIV_CON_ERR: {0}".format(e))
