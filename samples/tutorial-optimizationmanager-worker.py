import traceback
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
from pyalgotrade.optimizer.optimizationmanager import JobRequestParameters
from pyalgotrade.optimizer.optimizationmanager import ResultSubmitParameters
from pyalgotrade.optimizer.optimizationmanager import Job


def receiveFile(filename):
    frames = receiveSocket.recv_multipart()
    topic = frames.pop(0)
    data = frames.pop(0)
    with open(filename, 'w+b') as f:
        f.write(data)


def readFile(filename):
    with open(filename, "rb") as f:
        contents = f.read()
    return contents


def checkDataFile(myUid, checksum):
    filename = "data_" + checksum
    if not os.path.exists(filename):
        sendSocket.send_multipart(
            ["DATA_REQUEST",
             pickle.dumps([myUid, checksum])]
        )
        receiveFile(filename)
    return pickle.loads(readFile(filename))


def checkFeedFile(myUid, checksum):
    filename = "feed_" + checksum
    if not os.path.exists(filename):
        sendSocket.send_multipart(
            ["FEED_REQUEST",
             pickle.dumps([myUid, checksum])]
        )
        receiveFile(filename)
    return pickle.loads(readFile(filename))


def checkStratFile(myUid, checksum):
    filename = "strat_" + checksum
    if not os.path.exists(filename):
        sendSocket.send_multipart(
            ["STRAT_REQUEST",
             pickle.dumps([myUid, checksum])]
        )
        receiveFile(filename)
    return pickle.loads(readFile(filename))

if __name__ == '__main__':
    myUid = str(uuid.uuid4())

    jobRequestParams = JobRequestParameters(myUid)

    zmqContext = zmq.Context.instance()
    sendSocket = zmqContext.socket(zmq.PUB)
    sendSocket.connect("tcp://127.0.0.1:5002")

    receiveSocket = zmqContext.socket(zmq.SUB)
    receiveSocket.subscribe(str(myUid))
    receiveSocket.connect("tcp://127.0.0.1:5003")

    # Let the sockets connect
    time.sleep(5)

    inactivityCounter = 0
    while True:
        sendSocket.send_multipart(
            ["JOB_REQUEST",
             pickle.dumps(jobRequestParams)]
        )

        available = receiveSocket.poll(1000)
        if available > 0:
            # Reset the incativity counter
            inactivityCounter = 0

            # Do our stuff
            frames = receiveSocket.recv_multipart()
            topic = frames.pop(0)
            jobParams = pickle.loads(frames.pop(0))

            dataChecksum = jobParams.dataChecksum
            stratChecksum = jobParams.stratChecksum
            feedChecksum = jobParams.feedChecksum

            dataFilename = "data_" + dataChecksum
            stratFilename = "strat_" + stratChecksum
            feedFilename = "feed_" + feedChecksum

            data = checkDataFile(myUid, dataChecksum)
            strat = checkStratFile(myUid, stratChecksum)
            feed = checkFeedFile(myUid, feedChecksum)

            if True:
            # try:
                job = Job(jobParams.uid, jobParams.batchUid,
                          data, dataChecksum,
                          feed, strat, jobParams.params)
                job.run()

                userData = None
                if hasattr(job.strat, "_userData"):
                    userData = job.strat._userData

                resultSubmitParams = ResultSubmitParameters(
                    myUid, jobParams,
                    job.strat.getResult(),
                    userData)
                sendSocket.send_multipart(
                    ["SUBMIT_RESULTS",
                     pickle.dumps(resultSubmitParams)]
                )
            # except Exception as e:
            #     print "Worker {}, exception: {}\n".format(
            #         myUid, e.message)
            #     traceback.print_stack()
        else:
            # Waiting for the server to pass over any jobs
            inactivityCounter = inactivityCounter + 1
            if inactivityCounter > 60:
                # If we don't get any jobs within 60 seconds assume
                # that the connection became unavailable and try
                # connecting again.
                sendSocket.close()
                sendSocket = zmqContext.socket(zmq.PUB)
                sendSocket.connect("tcp://127.0.0.1:5002")
                receiveSocket.close()
                receiveSocket = zmqContext.socket(zmq.SUB)
                receiveSocket.subscribe(str(myUid))
                receiveSocket.connect("tcp://127.0.0.1:5003")
                time.sleep(5)
