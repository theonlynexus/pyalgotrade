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
        sendSocket.send("JOB_REQUEST", flags=zmq.SNDMORE)
        sendSocket.send_pyobj(jobRequestParams)

        available = receiveSocket.poll(1)
        if available > 0:
            # Reset the incativity counter
            inactivityCounter = 0

            # Do our stuff
            frames = receiveSocket.recv_multipart()
            topic = frames.pop(0)
            jobParams = pickle.loads(frames.pop(0))

            job = Job(jobParams)
            job.run()

            resultSubmitParams = ResultSubmitParameters(myUid,
                                                        jobParams,
                                                        job.strat.getResult())
            sendSocket.send("SUBMIT_RESULTS", zmq.SNDMORE)
            sendSocket.send_pyobj(resultSubmitParams)
        else:
            # Waiting for yo, my love...
            inactivityCounter = inactivityCounter + 1
            if inactivityCounter > 60:
                # Our love isn't replying, let's try placing
                # that phone call again
                sendSocket.close()
                sendSocket = zmqContext.socket(zmq.PUB)
                sendSocket.connect("tcp://127.0.0.1:5002")
                receiveSocket.close()
                receiveSocket.subscribe(str(myUid))
                receiveSocket.connect("tcp://127.0.0.1:5003")