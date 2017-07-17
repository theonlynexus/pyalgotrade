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
import fasteners
import md5
from tornado import web
from zmq.eventloop import ioloop


def pickleAndCompress(data):
    return zlib.compress(pickle.dumps(data))


def decompressAndUnpickle(string):
    return pickle.loads(zlib.decompress(string))


def md5digest(string):
    if isinstance(string, basestring):
        return md5.md5(string).hexdigest()
    else:
        return md5.md5(pickle.dumps(string)).hexdigest()


###################################################
class JobSubmitParameters():
    def __init__(self, uid, batchUid, submitter, description,
                 dataChecksum, feedChecksum, stratChecksum, paramSet):
        self.uid = uid
        self.batchUid = batchUid
        self.submitter = submitter
        self.description = description
        self.dataChecksum = dataChecksum
        self.feedChecksum = feedChecksum
        self.stratChecksum = stratChecksum
        self.paramSet = paramSet


###################################################
class JobRequestParameters():
    def __init__(self, workerUid):
        self.workerUid = workerUid


###################################################
class Job():
    def __init__(self, uid, batchUid,
                 data, dataChecksum,
                 feed, feedChecksum,
                 strat, stratChecksum,
                 paramSet):
        self.uid = uid
        self.batchUid = batchUid

        self.data = data
        self.dataChecksum = dataChecksum

        self.feedName = feed[0]
        self.feedCode = feed[1]
        self.feedChecksum = feedChecksum

        self.stratName = strat[0]
        self.stratCode = strat[1]
        self.stratChecksum = stratChecksum

        self.paramSet = paramSet

        self.feed = None
        self.strat = None

    def decorateParam(self, param):
        if isinstance(param, basestring):
            return '"{}"'.format(param)
        else:
            return str(param)

    def makeParamList(self, params):
        return ", ".join(map(lambda x: self.decorateParam(x), self.paramSet))

    def actuallyWriteFile(self, filename, code):
        checksumFilename = "md5_" + filename
        with open(filename, 'w+b') as f:
            f.write(code)
            f.close()
        with open(filename, 'rb') as f:
            fileChecksum = md5.md5(f.read()).hexdigest()
            f.close()
        with open(checksumFilename, 'w+b') as f:
            f.write(fileChecksum)
            f.close()

    def writeModule(self, filename, code, checksum):
        checksumFilename = "md5_" + filename
        lockFilename = "lock_" + checksum
        with fasteners.InterProcessLock(lockFilename):
            if not os.path.exists(checksumFilename):
                self.actuallyWriteFile(filename, code)
            else:
                with open(checksumFilename, 'rb') as f:
                    fileChecksum = f.read()
                    f.close()
                if fileChecksum != checksum:
                    self.actuallyWriteFile(filename, code)

    def loadModule(self, filename, modname, checksum):
        lockFilename = "lock_" + checksum
        with fasteners.InterProcessLock(lockFilename):
            try:
                (modfile, pathname, desc) = imp.find_module(modname, ["."])
                imp.load_module(modname, modfile, pathname, desc)
            finally:
                modfile.close()

    def run(self):
        modname = "feed_" + self.feedChecksum
        filename = modname + ".py"

        self.writeModule(filename,
                         self.feedCode,
                         self.feedChecksum)
        self.loadModule(filename, modname, self.feedChecksum)

        codeline = "import " + modname
        exec(codeline)
        codeline = "self.feed = " + modname + "." + self.feedName + "()"
        exec(codeline)

        i = 0
        for (instrument, data) in self.data:
            filename = "data_" + self.dataChecksum + "_" + str(i) + ".csv"
            if not os.path.exists(filename):
                with open(filename, 'w') as f:
                    f.write(zlib.decompress(data))
                    f.close()
            self.feed.addBarsFromCSV(instrument, filename)
            i = i + 1

        modname = "strat_" + self.stratChecksum
        filename = modname + ".py"
        self.writeModule(filename,
                         self.stratCode,
                         self.stratChecksum)
        self.loadModule(filename, modname, self.stratChecksum)

        codeline = "import " + modname
        exec(codeline)
        parCsv = self.makeParamList(self.paramSet)
        params = "(self.feed, " + parCsv + ")"
        codeline = "self.strat = " + modname + "." + self.stratName + params
        exec(codeline)

        self.strat.run()


###################################################
class BatchSubmitParameters():
    def __init__(self, uid, submitter, description,
                 data, dataChecksum,
                 feed, feedChecksum,
                 strat, stratChecksum,
                 paramGrid):
        self.uid = uid
        self.submitter = submitter
        self.description = description
        self.compressedData = pickleAndCompress(data)
        self.dataChecksum = dataChecksum
        self.compressedFeed = pickleAndCompress(feed)
        self.feedChecksum = feedChecksum
        self.compressedStrat = pickleAndCompress(strat)
        self.stratChecksum = stratChecksum
        self.compressedParamGrid = pickleAndCompress(paramGrid)


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
        self.userData = pickleAndCompress(userData)


###################################################
class Batch():
    def __init__(self, batchParameters):
        self.uid = batchParameters.uid
        self.submitter = batchParameters.submitter
        self.description = batchParameters.description
        self.compressedData = batchParameters.compressedData
        self.dataChecksum = batchParameters.dataChecksum
        self.compressedFeed = batchParameters.compressedFeed
        self.feedChecksum = batchParameters.feedChecksum
        self.compressedStrat = batchParameters.compressedStrat
        self.stratChecksum = batchParameters.stratChecksum
        self.compressedParamGrid = batchParameters.compressedParamGrid

        paramGrid = decompressAndUnpickle(self.compressedParamGrid)
        codeline = "paramIter = itertools.product("
        args = []
        for i in range(0, len(paramGrid)):
            args.append("paramGrid[{}]".format(i))
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

    def getNextParamSet(self):
        paramSet = None
        if len(self.paramGrid) > 0:
            paramSet = self.paramGrid.pop(0)
            self.processing.append(paramSet)
        elif len(self.processing) > 0:
            paramSet = self.processing[0]
        return paramSet

    def addResults(self, paramSet, returns, userData):
        if paramSet in self.processing:
            self.processing.remove(paramSet)
        if paramSet not in self.completed:
            self.completed.append(paramSet)
            self.returns[paramSet] = returns
            self.userData[paramSet] = userData

    def writeToDisk(self):
        dataFilename = "data_" + self.dataChecksum
        stratFilename = "strat_" + self.stratChecksum
        feedFilename = "feed_" + self.feedChecksum

        if not os.path.exists(dataFilename):
            with open(dataFilename, 'w+b') as f:
                f.write(self.compressedData)
        if not os.path.exists(stratFilename):
            with open(stratFilename, 'w+b') as f:
                f.write(self.compressedStrat)
        if not os.path.exists(feedFilename):
            with open(feedFilename, 'w+b') as f:
                f.write(self.compressedFeed)


###################################################
class MainHandler(tornado.web.RequestHandler):
    def initialize(self, batches, completeBatches):
        self._batches = batches
        self._completeBatches = completeBatches

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
        for batch in self._batches:
            self.write("<p>")
            self.write("Batch {}".format(batch.uid))
            self.write("<ul>")
            self.write("<li>Submitter: {}</li>".format(batch.submitter))
            self.write("<li>Description: {}</li>".format(batch.description))
            stratName = decompressAndUnpickle(batch.compressedStrat)[0]
            self.write("<li>Strategy: {}</li>".format(stratName))
            self.write("<li>Jobs - Remaining: {}, Pending: {}, "
                       "Completed: {}</li>".format(
                           len(batch.paramGrid), len(batch.processing),
                           len(batch.completed)))
            self.write("</ul>")
            self.write("</p>")

        self.write("<h3>Complete optimization batches</h3>")
        for batch in self._completeBatches:
            self.write("<p>")
            self.write("Batch {}".format(batch.uid))
            self.write("<ul>")
            self.write("<li>Submitter: {}</li>".format(batch.submitter))
            self.write("<li>Description: {}</li>".format(batch.description))
            stratName = decompressAndUnpickle(batch.compressedStrat)[0]
            self.write("<li>Strategy: {}</li>".format(stratName))
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
    def __init__(self, webIfAddr="127.0.0.1", webPort=5080,
                 clientIfAddr="127.0.0.1", clientRequestPort=5000,
                 clientReplyPort=5001,
                 workerIfAddr="127.0.0.1", workerRequestPort=5002,
                 workerReplyPort=5003,
                 maxCompleteBatches=10):
        super(OptimizationManager, self).__init__(
            group=None, name="OptimizationManager")

        # Interaction with submitters
        self._clientRequestSocket = None
        self._clientReplySocket = None

        # Interaction with workers
        self._workerRequestSocket = None
        self._resultsSubmitSocket = None

        self._poller = None

        self._zmqContext = zmq.Context.instance()

        self._router = None
        self._loop = None

        self._batches = []
        self._completeBatches = []

        self._maxResultsStored = maxCompleteBatches

        # This should really be replaced with the new CLIENT-SERVER socket
        # model available in later iterations of ZMQ

        # "Servers" (submitters) interface
        self.clientRequestSocket = self._zmqContext.socket(zmq.SUB)
        self.clientRequestSocket.bind(
            "tcp://" + str(clientIfAddr) + ":" + str(clientRequestPort))
        self.clientRequestSocket.subscribe("")
        self.clientReplySocket = self._zmqContext.socket(zmq.PUB)
        self.clientReplySocket.bind(
            "tcp://" + str(clientIfAddr) + ":" + str(clientReplyPort))

        # Workers interface
        self.workerRequestSocket = self._zmqContext.socket(zmq.SUB)
        self.workerRequestSocket.bind(
            "tcp://" + str(workerIfAddr) + ":" + str(workerRequestPort))
        self.workerRequestSocket.subscribe("")
        self.workerReplySocket = self._zmqContext.socket(zmq.PUB)
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
            (r"/", MainHandler, dict(batches=self._batches,
                                     completeBatches=self._completeBatches)),
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
        # print("PUBLISH_RESULTS".format(uid))
        # print("Publishing results for batch: {}".format(uid))
        self.clientReplySocket.send(uid,
                                    flags=zmq.SNDMORE)
        self.clientReplySocket.send_pyobj(params)

    def processClientRequest(self, topicFrame, paramsFrame):
        if topicFrame == "SUBMIT_BATCH":
            params = pickle.loads(paramsFrame)
            # print("SUBMIT_BATCH from client {}, uid: {}, "
            #       "description:{}".format(params.submitter, params.uid,
            #                               params.description))
            batch = Batch(params)
            self._batches.append(batch)
            batch.writeToDisk()

        if topicFrame == "REQUEST_RESULTS":
            params = pickle.loads(paramsFrame)
            # print("REQUEST_RESULTS from client {}, submitter: {}, "
            #       "description:{}".format(params.uid, params.submitter))
            replyParams = batchResultsReplyParameters(None, None, None)
            for batch in self._completeBatches:
                if batch.uid == params.uid:
                    replyParams = batchResultsReplyParameters(
                        batch.uid,
                        batch.paramGrid,
                        batch.returns
                    )
            self.publishBatchResults(params.uid, replyParams)

    def _serveFile(self, workerUid, filename):
        with open(filename) as f:
            contents = f.read()
        self.workerReplySocket.send_multipart(
            [workerUid, contents]
        )

    def processWorkerRequest(self, topicFrame, paramsFrame):
        if len(self._completeBatches) > self._maxResultsStored:
            self._completeBatches.pop(0)

        if topicFrame == "SUBMIT_RESULTS":
            params = pickle.loads(paramsFrame)
            # print("SUBMIT_RESULTS from worker: {}".format(params.workerUid))
            for batch in self._batches:
                if batch.uid == params.jobParams.batchUid:
                    batch.addResults(
                        params.jobParams.paramSet,
                        params.returns, 
                        params.userData)

        if topicFrame == "DATA_REQUEST":
            params = pickle.loads(paramsFrame)
            workerUid = params[0]
            checksum = params[1]
            filename = "data_" + checksum
            self._serveFile(workerUid, filename)

        if topicFrame == "FEED_REQUEST":
            params = pickle.loads(paramsFrame)
            workerUid = params[0]
            checksum = params[1]
            filename = "feed_" + checksum
            self._serveFile(workerUid, filename)

        if topicFrame == "STRAT_REQUEST":
            params = pickle.loads(paramsFrame)
            workerUid = params[0]
            checksum = params[1]
            filename = "strat_" + checksum
            self._serveFile(workerUid, filename)

        if topicFrame == "JOB_REQUEST":
            params = pickle.loads(paramsFrame)
            # print("JOB_REQUEST from worker: {}".format(params.workerUid))

            if len(self._batches) > 0:
                # Distribute any non-pending workload
                batch = self._batches[0]
                paramSet = batch.getNextParamSet()
                if paramSet is not None:
                    jobParams = JobSubmitParameters(uuid.uuid4(),
                                                    batch.uid,
                                                    batch.submitter,
                                                    batch.description,
                                                    batch.dataChecksum,
                                                    batch.feedChecksum,
                                                    batch.stratChecksum,
                                                    paramSet)
                    self.workerReplySocket.send(params.workerUid,
                                                flags=zmq.SNDMORE)
                    self.workerReplySocket.send_pyobj(jobParams)
                    # print("Sending batch [{}, {}, {}] "
                    #       "to worker: {}".format(jobParams.batchUid,
                    #                              jobParams.strat[0],
                    #                              jobParams.params,
                    #                              params.workerUid))
                else:
                    # Both our non-pending and pending job lists are empty,
                    # we're done: open a bottle of spumante!
                    batch = self._batches.pop(0)
                    self._completeBatches.append(batch)
                    replyParams = BatchResultsReplyParameters(
                        batch.uid,
                        batch.paramGrid,
                        batch.returns,
                        batch.userData
                    )
                    self.publishBatchResults(batch.uid, replyParams)

    def clientRequestHandler(self, socket, events):
        # print("Client Request")
        frames = socket.recv_multipart()
        if len(frames) < 2:
            raise Exception("Client request too short")
        elif len(frames) > 2:
            raise Exception("Client request too long")
        topic = frames.pop(0)
        params = frames.pop(0)
        self.processClientRequest(topic, params)

    def workerRequestHandler(self, socket, events):
        # print("Worker Request")
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
