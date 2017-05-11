import sys
sys.path = ["."] + sys.path
import itertools
import json
import zmq
import pickle
from pyalgotrade.optimizer.optimizationmanager import OptimizationManager
from pyalgotrade.optimizer.optimizationmanager import BatchSubmitParameters

if __name__ == '__main__':
    manager = OptimizationManager()
    manager.start()


