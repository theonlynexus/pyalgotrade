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

if __name__ == '__main__':
    myUid = str(uuid.uuid4())

    jobRequestParams = JobRequestParameters(myUid)

    zmqContext = zmq.Context.instance()
    sendSocket = zmqContext.socket(zmq.PUB)
    sendSocket.connect("tcp://192.168.0.38:5002")

    receiveSocket = zmqContext.socket(zmq.SUB)
    receiveSocket.subscribe(str(myUid))
    receiveSocket.connect("tcp://192.168.0.38:5003")

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

            try:
                job = Job(jobParams)
                job.run()
                resultSubmitParams = ResultSubmitParameters(
                    myUid, jobParams,
                    job.strat.getResult(),
                    job.strat._userData)
                sendSocket.send_multipart(
                    ["SUBMIT_RESULTS",
                     pickle.dumps(resultSubmitParams)]
                )
            except Exception as e:
                print "Worker {}, exception: {}\nStacktrace: {}".format(
                    myUid, e.message, repr(traceback.format_stack())
                )
        else:
            # Waiting for the server to pass over any jobs
            inactivityCounter = inactivityCounter + 1
            if inactivityCounter > 60:
                # If we don't get any jobs within 60 seconds assume
                # that the connection became unavailable and try
                # connecting again.
                sendSocket.close()
                sendSocket = zmqContext.socket(zmq.PUB)
                sendSocket.connect("tcp://192.168.0.38:5002")
                receiveSocket.close()
                receiveSocket = zmqContext.socket(zmq.SUB)
                receiveSocket.subscribe(str(myUid))
                receiveSocket.connect("tcp://192.168.0.38:5003")
                time.sleep(5)
