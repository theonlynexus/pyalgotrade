import traceback
import itertools
import json
import uuid
import zmq
import pickle
import os
import time
import zlib
from pyalgotrade.barfeed import yahoofeed
from pyalgotrade.optimizer.optimizationmanager import JobRequestParameters
from pyalgotrade.optimizer.optimizationmanager import ResultSubmitParameters
from pyalgotrade.optimizer.optimizationmanager import Job
from pyalgotrade.optimizer.optimizationmanager import decompressAndUnpickle
from pyalgotrade.optimizer.optimizationmanager import pickleAndCompress


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
    return decompressAndUnpickle(readFile(filename))


def checkFeedFile(myUid, checksum):
    filename = "feed_" + checksum
    if not os.path.exists(filename):
        sendSocket.send_multipart(
            ["FEED_REQUEST",
             pickle.dumps([myUid, checksum])]
        )
        receiveFile(filename)
    return decompressAndUnpickle(readFile(filename))


def checkStratFile(myUid, checksum):
    filename = "strat_" + checksum
    if not os.path.exists(filename):
        sendSocket.send_multipart(
            ["STRAT_REQUEST",
             pickle.dumps([myUid, checksum])]
        )
        receiveFile(filename)
    return decompressAndUnpickle(readFile(filename))


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

            dataFilename = "data_" + jobParams.dataChecksum
            stratFilename = "strat_" + jobParams.stratChecksum
            feedFilename = "feed_" + jobParams.feedChecksum

            data = checkDataFile(myUid, jobParams.dataChecksum)
            strat = checkStratFile(myUid, jobParams.stratChecksum)
            feed = checkFeedFile(myUid, jobParams.feedChecksum)

            try:
                if True:
                    job = Job(
                        jobParams.uid, jobParams.batchUid,
                        data, jobParams.dataChecksum,
                        feed, jobParams.feedChecksum,
                        strat, jobParams.stratChecksum,
                        jobParams.paramSet)
                    job.run()

                    userData = None
                    if hasattr(job.strat, "_userData"):
                        userData = job.strat._userData

                    resultSubmitParams = ResultSubmitParameters(
                        myUid,
                        jobParams,
                        job.strat.getResult(),
                        userData)
                    sendSocket.send_multipart(
                        ["SUBMIT_RESULTS",
                         pickle.dumps(resultSubmitParams)]
                    )
            except Exception as e:
                print "Worker {}, exception: [{}] {}\n".format(
                    myUid, e.errno, e.strerror)
                print(traceback.format_exc())
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
