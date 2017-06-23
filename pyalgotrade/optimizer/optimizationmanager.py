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

# http://stackoverflow.com/questions/34709390/how-can-i-import-a-pyc-compiled-python-file-and-use-it

"""
.. moduleauthor:: Massimo Fierro <massimo.fierro@gmail.com>
"""
import uuid
import zmq
import marshal
import zlib
import threading
import time
import pickle
import json
import imp
import itertools
import os
import random
import string
import tornado
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
        self.uid = jobParameters.uid
        self.batchUid = jobParameters.batchUid

        self.data = jobParameters.data

        self.feedName = jobParameters.feed[0]
        self.feedCode = jobParameters.feed[1]

        self.stratName = jobParameters.strat[0]
        self.stratCode = jobParameters.strat[1]

        self.params = jobParameters.params

        self.feed = None
        self.strat = None

    def decorateParam(self, param):
        if isinstance(param, basestring):
            return '"{}"'.format(param)
        else:
            return str(param)

    def makeParamList(self, params):
        return ", ".join(map(lambda x: self.decorateParam(x), self.params))

    def run(self):
        modname = "feed_" + string.replace(self.batchUid, "-", "_")
        filename = modname + ".py"
        with open(filename, 'wb') as f:
            f.write(zlib.decompress(self.feedCode))
            f.close()
        try:
            (modfile, pathname, desc) = imp.find_module(modname, ["."])
            imp.load_module(modname, modfile, pathname, desc)
        finally:
            modfile.close()
        codeline = "import " + modname
        exec(codeline)
        codeline = "self.feed = " + modname + "." + self.feedName + "()"
        exec(codeline)

        i = 0
        for (instrument, data) in self.data:
            filename = self.batchUid + "_data_" + str(i) + ".csv"
            if not os.path.exists(filename):
                with open(filename, 'w') as f:
                    f.write(zlib.decompress(data))
                    f.close()
            self.feed.addBarsFromCSV(instrument, filename)
            i = i + 1

        modname = "strat_" + string.replace(self.batchUid, "-", "_")
        filename = modname + ".py"
        with open(filename, 'w') as f:
            f.write(zlib.decompress(self.stratCode))
            f.close()
        try:
            (modfile, pathname, desc) = imp.find_module(modname, ["."])
            imp.load_module(modname, modfile, pathname, desc)
        finally:
            modfile.close()
        codeline = "import " + modname
        exec(codeline)
        parCsv = self.makeParamList(self.params)
        params = "(self.feed, " + parCsv + ")"
        codeline = "self.strat = " + modname + "." + self.stratName + params
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
class BatchResultsRequestParameters():
    def __init__(self, uid, submitter):
        self.uid = uid
        self.submitter = submitter


###################################################
class BatchResultsReplyParameters():
    def __init__(self, uid, paramGrid, results, userData=None):
        self.uid = uid
        self.paramGrid = paramGrid
        self.results = results
        self.userData = userData


###################################################
class ResultSubmitParameters():
    def __init__(self, workerUid, jobParams, returns, userData=None):
        self.workerUid = workerUid
        self.jobParams = jobParams
        self.returns = returns
        self.userData = userData


###################################################
class Batch():
    def __init__(self, batchParameters):
        self.uid = batchParameters.uid
        self.submitter = batchParameters.submitter
        self.description = batchParameters.description
        self.data = batchParameters.data
        self.feed = batchParameters.feed
        self.strat = batchParameters.strat

        codeline = "paramIter = itertools.product("
        args = []
        for i in range(0, len(batchParameters.paramGrid)):
            args.append("batchParameters.paramGrid[{}]".format(i))
        codeline = codeline + ", ".join(args) + ")"
        exec(codeline)

        self.paramGrid = []

        i = 0
        for paramSet in paramIter:
            self.paramGrid.append(paramSet)
            i = i + 1

        self.completed = []

        self.processing = []

        self.returns = dict()
        self.userData = dict()


###################################################
class MainHandler(tornado.web.RequestHandler):
    def initialize(self, batches, completeBatches):
        self.batches = batches
        self.completeBatches = completeBatches

    def getMax(self, iterable):
        if len(iterable) == 0:
            return None

        maxVal = None
        maxIdx = None
        for key, value in iterable.items():
            if maxVal is None or value > maxVal:
                maxVal = value
                maxIdx = key
        return (maxVal, maxIdx)

    def get(self):
        self.write("<h3>Pending optimization batches</h3>")
        for batch in self.batches:
            self.write("<p>")
            self.write("Batch {}".format(batch.uid))
            self.write("<ul>")
            self.write("<li>Submitter: {}</li>".format(batch.submitter))
            self.write("<li>Description: {}</li>".format(batch.description))
            self.write("<li>Strategy: {}</li>".format(batch.strat[0]))
            self.write("<li>Jobs - Remaining: {}, Pending: {}, "
                       "Completed: {}</li>".format(
                           len(batch.paramGrid), len(batch.processing),
                           len(batch.completed)))
            self.write("</ul>")
            self.write("</p>")

        self.write("<h3>Complete optimization batches</h3>")
        for batch in self.completeBatches:
            self.write("<p>")
            self.write("Batch {}".format(batch.uid))
            self.write("<ul>")
            self.write("<li>Submitter: {}</li>".format(batch.submitter))
            self.write("<li>Description: {}</li>".format(batch.description))
            self.write("<li>Strategy: {}</li>".format(batch.strat[0]))
            (maxVal, maxParams) = self.getMax(batch.returns)
            self.write("<li>Best returns: {}</li>".format(maxVal))
            self.write("<li>... with parameters: {}</li>".format(maxParams))
            self.write("</ul>")
            self.write("</p>")


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

    batches = []
    completeBatches = []

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

        # Setup poller
        self.poller = zmq.Poller()
        self.poller.register(self.clientRequestSocket, zmq.POLLIN)
        self.poller.register(self.workerRequestSocket, zmq.POLLIN)

        # Setup Tornado loop
        ioloop.install()
        self.loop = ioloop.IOLoop.instance()
        self.loop.add_handler(self.clientRequestSocket,
                              self.clientRequestHandler, zmq.POLLIN)
        self.loop.add_handler(self.workerRequestSocket,
                              self.workerRequestHandler, zmq.POLLIN)

        # Setup Tornado application (the web interface)
        self.application = tornado.web.Application([
            (r"/", MainHandler, dict(batches=self.batches,
                                     completeBatches=self.completeBatches)),
        ])
        self.application.listen(webPort)

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

    def publishBatchResults(self, uid, params):
        print("PUBLISH_RESULTS".format(uid))
        print("Publishing results for batch: {}".format(uid))
        self.clientReplySocket.send(uid,
                                    flags=zmq.SNDMORE)
        self.clientReplySocket.send_pyobj(params)

    def processClientRequest(self, topicFrame, paramsFrame):
        if topicFrame == "SUBMIT_BATCH":
            params = pickle.loads(paramsFrame)
            print("SUBMIT_BATCH from client {}, uid: {}, "
                  "description:{}".format(params.submitter, params.uid,
                                          params.description))
            batch = Batch(params)
            self.batches.append(batch)
        elif topicFrame == "REQUEST_RESULTS":
            params = pickle.loads(paramsFrame)
            print("REQUEST_RESULTS from client {}, submitter: {}, "
                  "description:{}".format(params.uid, params.submitter))
            replyParams = batchResultsReplyParameters(None, None, None)
            for batch in self.completeBatches:
                if batch.uid == params.uid:
                    replyParams = batchResultsReplyParameters(
                        batch.uid,
                        batch.paramGrid,
                        batch.returns
                    )
            self.publishBatchResults(params.uid, replyParams)

    def processWorkerRequest(self, topicFrame, paramsFrame):
        if topicFrame == "SUBMIT_RESULTS":
            params = pickle.loads(paramsFrame)
            print("SUBMIT_RESULTS from worker: {}".format(params.workerUid))
            for batch in self.batches:
                if batch.uid == params.jobParams.batchUid:
                    batch.returns[params.jobParams.params] = params.returns
                    batch.userData[params.jobParams.params] = params.userData
                    if params.jobParams.params in batch.processing:
                        batch.processing.remove(params.jobParams.params)
                        batch.completed.append(params.jobParams.params)                        
                        print("Returns for job: {} = {}".format(
                            params.jobParams.uid, params.returns))

        if topicFrame == "JOB_REQUEST":
            params = pickle.loads(paramsFrame)
            print("JOB_REQUEST from worker: {}".format(params.workerUid))

            if len(self.batches) > 0:
                # Distribute any non-pending workload
                batch = self.batches[0]
                if len(batch.paramGrid) > 0:
                    jobParams = JobSubmitParameters(uuid.uuid4(),
                                                    batch.uid,
                                                    batch.submitter,
                                                    batch.description,
                                                    batch.data,
                                                    batch.feed,
                                                    batch.strat,
                                                    batch.paramGrid[0])
                    batch.processing.append(batch.paramGrid.pop(0))
                    self.workerReplySocket.send(params.workerUid,
                                                flags=zmq.SNDMORE)
                    self.workerReplySocket.send_pyobj(jobParams)
                    print("Sending batch [{}, {}, {}] "
                          "to worker: {}".format(jobParams.batchUid,
                                                 jobParams.strat[0],
                                                 jobParams.params,
                                                 params.workerUid))
                elif len(batch.paramGrid) == 0 and len(batch.processing) > 0:
                    # Re-distribute pending workload and see if anyone
                    # returns results faster
                    idx = random.randrange(0, len(batch.processing))
                    jobParams = JobSubmitParameters(uuid.uuid4(),
                                                    batch.uid,
                                                    batch.submitter,
                                                    batch.description,
                                                    batch.data,
                                                    batch.feed,
                                                    batch.strat,
                                                    batch.processing[idx])
                    self.workerReplySocket.send(params.workerUid,
                                                flags=zmq.SNDMORE)
                    self.workerReplySocket.send_pyobj(jobParams)
                    print("Sending job [{}, {}, {}] "
                          "to worker: {}".format(jobParams.batchUid,
                                                 jobParams.strat[0],
                                                 jobParams.params,
                                                 params.workerUid))
                else:
                    # Both our non-pending and pending job lists are empty,
                    # we're done: open a bottle of spumante!
                    batch = self.batches.pop(0)
                    self.completeBatches.append(batch)
                    replyParams = BatchResultsReplyParameters(
                        batch.uid,
                        batch.paramGrid,
                        batch.returns,
                        batch.userData
                    )
                    self.publishBatchResults(batch.uid, replyParams)

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
        self.processWorkerRequest(topic, params)

    def start(self):
        self.loop.start()

    def stop(self):
        self.loop.stop()
