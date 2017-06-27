"""
.. moduleauthor:: Massimo Fierro <massimo.fierro@ghrholdings.com>
"""

import threading
from time import sleep
import zmq


class ZmqClientThread(threading.Thread):
    def __init__(self, url, topics=[""]):
        super(ZmqClientThread, self).__init__()
        # self.__queue = []
        self.__url = url
        self._topics = topics
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.SUB)
        self.__poller = None
        self.__connected = False
        self.__lastMessageInstrument = None
        self.__recvCallback = None
        self.__idleCallback = None
        self.__started = False

    def __connect(self):
        try:
            if self.__socket is None:
                print "ERROR: ZmqClientThread.connect() -> Socket is None!!!"
            self.__socket.connect(self.__url)
            for topic in self._topics:
                self.__socket.setsockopt(zmq.SUBSCRIBE, topic)
        except zmq.ZMQError as zmqerror:
            print "Error: " + zmqerror.message
            return False
        self.__poller = zmq.Poller()
        self.__poller.register(self.__socket, zmq.POLLIN)
        return True

    def __disconnect(self):
        if self.__socket is not None:
            self.__socket.disconnect(self.__url)

    def start(self):
        if not self.__connected:
            if self.__connect():
                self.__started = True
            else:
                self.__started = False
        super(ZmqClientThread, self).start()

    def run(self):
        while self.__started:
            socks = dict(self.__poller.poll(timeout=10))
            if self.__socket in socks and socks[self.__socket] == zmq.POLLIN:
                message = self.__socket.recv_multipart()
                if self.__recvCallback is not None:
                    self.__recvCallback(str(message[0]), str(message[1]))
            else:
                if self.__idleCallback is not None:
                    self.__idleCallback()
                sleep(.025)

    def stop(self):
        self.__started = False
        self.__disconnect()

    def setRecvCallback(self, callback):
        self.__recvCallback = callback

    def setIdleCallback(self, callback):
        self.__idleCallback = callback
