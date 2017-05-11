# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Massimo Fierro <massimo.fierro@gmail.com>
"""
import sys
sys.path = ["."] + sys.path
import uuid
import zmq
import zlib
import threading
import time
import pickle
import json
import itertools
from tornado import web
from zmq.eventloop import ioloop


###################################################
class JobSubmitParameters():
    def __init__(self, uid, batchUid, submitter, description,
                 data, feed, strat, params):
        self.uid = uid
        self.batchUid = batchUid
        self.submitter = submitter
        self.description = description
        self.data = data
        self.feed = feed
        self.strat = strat
        self.params = params


###################################################
class JobRequestParameters():
    def __init__(self, workerUid):
        self.workerUid = workerUid


###################################################
class Job():
    def __init__(self, jobParameters):
        self.uid = jobParamters.uid
        self.batchUid = jobParamters.batchUid

        self.data = jobParameters.data

        self.feedName = jobParamters.feed[0]
        self.feedCode = jobParameters.feed[1]

        self.stratName = jobParamters.strat[0]
        self.stratCode = jobParamters.strat[1]

        self.params = jobParameters.params

    def run(self):
        exec(self.feedCode)
        codeline = "self.feed = " + self.feedName + "()"
        exec(codeline)

        i = 0
        for (instrument, data) in jobParameters.data:
            filename = self.batchUid + "_data_" + str(i) + ".csv"
            if not os.path.exists(filename):
                with open(filename, 'w') as f:
                    f.write(zlib.decompress(data))
            self.feed.addBarsFromCSV(instrument, filename)
            i = i + 1

        exec(self.stratCode)
        codeline = "self.strat = " + self.stratName + "(self.feed, " + str(
            *self.params) + ")"
        exec(codeline)

        self.strat.run()


###################################################
class BatchSubmitParameters():
    def __init__(self, uid, submitter, description,
                 data, feed, strat, paramGrid):
        self.uid = uid
        self.submitter = submitter
        self.description = description
        self.data = data
        self.feed = feed
        self.strat = strat
        self.paramGrid = paramGrid


###################################################
class Batch():
    def __init__(self, batchParameters):
        self.uid = batchParameters.uid
        self.submitter = batchParameters.submitter
        self.description = batchParameters.description

        self.data = batchParameters.data

        self.feedName = batchParameters.feed[0]
        self.feedCode = batchParameters.feed[1]

        self.stratName = batchParameters.strat[0]
        self.stratCode = batchParameters.strat[1]

        codeline = "paramIter = itertools.product("
        args = []
        for i in range(0, len(batchParameters.paramGrid)):
            args.append("batchParameters.paramGrid[{}]".format(i))
        codeline = codeline + ", ".join(args) + ")"
        exec(codeline)

        self.paramGrid = []
        for paramSet in paramIter:
            self.paramGrid.append(paramSet)

        self.completed = []


###################################################
class OptimizationManager(threading.Thread):
    """Optimization manager: receives jobs from submitters, distributes them
    to workers.

    This is intended as a centralized manager (orchestrator) of the
    optimization process. It will accept a certain batch from a client
    (strategy, data and a parameter "table") then distribute each job to any
    worker that requests one. Results will be stored locally for further
    consumption by the clients.
    """

    # Interaction with submitters
    clientRequestSocket = None
    clientReplySocket = None

    # Interaction with workers
    workerRequestSocket = None
    resultsSubmitSocket = None

    poller = None

    zmqContext = zmq.Context.instance()

    router = None
    loop = None

    self.batches = []
    self.completeBatches = []

    def __init__(self, webIfAddr="127.0.0.1", webPort=5080,
                 clientIfAddr="127.0.0.1", clientRequestPort=5000,
                 clientReplyPort=5001,
                 workerIfAddr="127.0.0.1", workerRequestPort=5002,
                 workerReplyPort=5003):
        super(OptimizationManager, self).__init__(
            group=None, name="OptimizationManager")

        # This should really be replaced with the new CLIENT-SERVER socket
        # model available in later iterations of ZMQ

        # "Servers" (submitters) interface
        self.clientRequestSocket = self.zmqContext.socket(zmq.SUB)
        self.clientRequestSocket.bind(
            "tcp://" + str(clientIfAddr) + ":" + str(clientRequestPort))
        self.clientRequestSocket.subscribe("")
        self.clientReplySocket = self.zmqContext.socket(zmq.PUB)
        self.clientReplySocket.bind(
            "tcp://" + str(clientIfAddr) + ":" + str(clientReplyPort))

        # Workers interface
        self.workerRequestSocket = self.zmqContext.socket(zmq.SUB)
        self.workerRequestSocket.bind(
            "tcp://" + str(workerIfAddr) + ":" + str(workerRequestPort))
        self.workerRequestSocket.subscribe("")
        self.workerReplySocket = self.zmqContext.socket(zmq.PUB)
        self.workerReplySocket.bind(
            "tcp://" + str(workerIfAddr) + ":" + str(workerReplyPort))

        self.poller = zmq.Poller()
        self.poller.register(self.clientRequestSocket, zmq.POLLIN)
        self.poller.register(self.workerRequestSocket, zmq.POLLIN)

        # Init Tornado loop
        ioloop.install()
        self.loop = ioloop.IOLoop.instance()

        self.loop.add_handler(self.clientRequestSocket,
                              self.clientRequestHandler, zmq.POLLIN)
        self.loop.add_handler(self.workerRequestSocket,
                              self.workerRequestHandler, zmq.POLLIN)

    def __enter__(self):
        return self

    def _shutdown(self):
        if self.clientRequestSocket is not None:
            self.clientRequestSocket.close()
            self.clientRequestSocket = None
        if self.workerRequestSocket is not None:
            self.workerRequestSocket.close()
            self.workerRequestSocket = None
        if self.clientReplySocket is not None:
            self.workerRequestSocket.close()
            self.workerRequestSocket = None

    def __exit__(self, exc_type, exc_value, traceback):
        self._shutdown()

    def processClientRequest(self, topicFrame, paramsFrame):
        if topicFrame == "SUBMIT_BATCH":
            params = pickle.loads(paramsFrame)
            print("{}, {}, {}".format(
                params.uid, params.submitter, params.description))
            batch = Batch(params)
            self.batches.append(batch)

    def processWorkerRequest(self, topicFrame, paramsFrame):
        if topicFrame == "JOB_REQUEST":
            params = pickle.loads(paramsFrame)
            if len(self.batches) > 0:
                batch = self.batches[0]
                if len(batch.paramGrid) > 0:
                    jobParams = JobSubmitParameters(uuid.uuid4(),
                                                    batch.batchUid,
                                                    batch.submitter,
                                                    batch.description,
                                                    batch.data,
                                                    batch.feed,
                                                    batch.strat,
                                                    batch.paramGrid[0])
                    self.workerReplySocket.send(params.workerUid,
                                                flags=zmq.SNDMORE)
                    self.workerReplySocket.send_pyobj(jobParams)
                else:
                    batch.completed.append(batch.paramGrid.pop())

    def clientRequestHandler(self, socket, events):
        print("Client Request")
        frames = socket.recv_multipart()
        if len(frames) < 2:
            raise Exception("Client request too short")
        elif len(frames) > 2:
            raise Exception("Client request too long")
        topic = frames.pop(0)
        params = frames.pop(0)
        self.processClientRequest(topic, params)

    def workerRequestHandler(self, socket, events):
        print("Worker Request")
        frames = socket.recv_multipart()
        if len(frames) < 2:
            raise Exception("Worker request too short")
        elif len(frames) > 2:
            raise Exception("Worker request too long")
        topic = frames.pop(0)
        params = frames.pop(0)
        self.processClientRequest(topic, params)

    def start(self):
        self.loop.start()

    def stop(self):
        self.loop.stop()
