import itertools
import json
import zmq
import pickle
from pyalgotrade.optimizer.optimizationmanager import OptimizationManager
from pyalgotrade.optimizer.optimizationmanager import BatchSubmitParameters

if __name__ == '__main__':
    manager = OptimizationManager(
	webIfAddr="192.168.0.38",
	webPort=5080,
        clientIfAddr="192.168.0.38",
	clientRequestPort=5000,
        clientReplyPort=5001,
        workerIfAddr="192.168.0.38",
	workerRequestPort=5002,
        workerReplyPort=5003)
    manager.start()


