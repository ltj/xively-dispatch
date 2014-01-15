import sys
import logging
import serial
import xively
import requests
from config import XIVELY_API_KEY, XIVELY_FEED_ID, LOG_FORMAT, DEBUG
from node import DispatchFactory


api = xively.XivelyAPIClient(XIVELY_API_KEY)
feed = api.feeds.get(XIVELY_FEED_ID)

def main(device='/dev/ttyUSB0', baudrate=57600, timeout=60):

    if DEBUG:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(filename='dispatch.log', filemode='w', format=LOG_FORMAT, level=level)

    try:
        df = DispatchFactory(XIVELY_API_KEY, XIVELY_FEED_ID)
    except requests.ConnectionError:
        logging.error("Unable to connect")
        sys.exit(1)
    except requests.Timeout:
        logging.error("Connection timeout")
        sys.exit(1)

    try:
        ser = serial.Serial(device, baudrate, timeout=timeout)
    except serial.SerialException:
        logging.error("Could not open serial port")
        sys.exit(1)

    logging.info("-- Xively dispatch started --")
    if DEBUG:
        logging.info("DEBUG MODE!")

    while True:
        line = ser.readline().decode("utf-8", "ignore")
        if DEBUG:
            logging.debug("SER_LINE: " + line)

        if line.startswith('OK'): # filter out noise
            df.handleNewPacket(line)


if __name__ == "__main__":
    args = sys.argv[1:]
    main(*args)
