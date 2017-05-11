import sys
sys.path = ["."] + sys.path
import itertools
import json
import uuid
from rsi2 import RSI2
import zmq
import pickle
import os
import time
import zlib
from pyalgotrade.barfeed import yahoofeed
from pyalgotrade.optimizer.optimizationmanager import BatchSubmitParameters

if __name__ == '__main__':
    # Notice how we can pass multiple intruments and multiple
    # files per instrument by using an array (iterable) of tuples.
    # It is important to pass files so that data is in chronological
    # order.

    with open("dia-2009.csv", 'r') as f:
        dia1 = zlib.compress(f.read())
    with open("dia-2010.csv", 'r') as f:
        dia2 = zlib.compress(f.read())
    with open("dia-2011.csv", 'r') as f:
        dia3 = zlib.compress(f.read())

    with open("samples/rsi2.py", 'r') as f:
        stratCode = zlib.compress(f.read())

    with open(os.path.abspath(yahoofeed.__file__), 'r') as f:
        feedCode = zlib.compress(f.read())

    paramGrid = [
        ["dia"],
        range(150, 251),
        range(5, 16),
        range(2, 11),
        range(75, 96),
        range(5, 26)]

    params = BatchSubmitParameters(str(uuid.uuid4()),
                             "Me",
                             "RSI2 test",
                             [("dia", dia1),
                              ("dia", dia2),
                              ("dia", dia3)],
                             ["Feed", feedCode],
                             ["RSI2", stratCode],
                             paramGrid)

    zmqContext = zmq.Context.instance()
    sendSocket = zmqContext.socket(zmq.PUB)
    sendSocket.connect("tcp://127.0.0.1:5000")

    time.sleep(1)

    sendSocket.send("SUBMIT_BATCH", flags=zmq.SNDMORE)
    sendSocket.send_pyobj(params)
