import itertools
import json
import zmq
import pickle
from pyalgotrade.optimizer.optimizationmanager import OptimizationManager
from pyalgotrade.optimizer.optimizationmanager import BatchSubmitParameters

if __name__ == '__main__':
    manager = OptimizationManager(
        webIfAddr="127.0.0.1",
        webPort=5080,
        clientIfAddr="127.0.0.1",
        clientRequestPort=5000,
        clientReplyPort=5001,
        workerIfAddr="127.0.0.1",
        workerRequestPort=5002,
        workerReplyPort=5003)
    manager.start()
