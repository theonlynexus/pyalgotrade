from sockjs.tornado import SockJSRouter, SockJSConnection
from tornado import web, ioloop
import json

clients = []

# Create a dictionary so that we can do something like
# lastBars['CL']['1m_bar'] or lastBars['CL']['1h_bar']
# to store the latest bar for each product/timespan.
lastTimestamp = None
lastBars = dict()


class ClientConnection(SockJSConnection):
    def on_open(self, request):
        clients.append(self)
        return True

    def on_message(self, msg):
        self.send(msg)

    def on_close(self):
        clients.remove(self)

    # def check_origin(self, origin):
    #     # let tornado first check if connection from the same domain
    #     same_domain = super(EchoConnection, self).check_origin(origin)
    #     if same_domain:
    #         return True


class IndexHandler(web.RequestHandler):
    def get(self):
        self.render('sockjs.html')


def updateLastBars(timeStamp):
    # Here we will change the timestamp
    # of any bars stores in lastBars, which will
    # possibly be overwritten on OnBarsHandler.post()
    pass


class OnBarsHandler(web.RequestHandler):
    router = None
    i = 0

    def initialize(self, router):
        self.router = router

    def post(self):
        obj = json.loads(self.request.body)

        for client in clients:
            if client.is_closed:
                clients.remove(client)
            else:
                client.send(self.request.body)
        print "[{}] {}".format(OnBarsHandler.i, self.request.body) 
        OnBarsHandler.i = OnBarsHandler.i+1

    def put(self):
        # TODO(max): sanity checks, security checks
        for client in clients:
            if client.is_closed:
                clients.remove(client)
            else:
                client.send(self.request.body)

if __name__ == '__main__':
    ClientRouter = SockJSRouter(ClientConnection, '/echo')

    app = web.Application(
        [(r"/", IndexHandler)] +
        [(r"/onbars", OnBarsHandler, dict(router=ClientRouter))] +
        ClientRouter.urls)
    app.listen(9999)
    ioloop.IOLoop.instance().start()
