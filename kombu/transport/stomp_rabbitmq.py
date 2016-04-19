import stomp

from . import virtual
from kombu.five import items
from kombu.transport import base

DEFAULT_PORT = 61613


class NA(object):
    pass


class Channel(base.StdChannel):
    """
    The RabbitMQ STOMP adapter supports a number of different destination types:

    /exchange -- SEND to arbitrary routing keys and SUBSCRIBE to arbitrary binding patterns;
    /queue -- SEND and SUBSCRIBE to queues managed by the STOMP gateway;
    /amq/queue -- SEND and SUBSCRIBE to queues created outside the STOMP gateway;
    /topic -- SEND and SUBSCRIBE to transient and durable topics;
    /temp-queue/ -- create temporary queues (in reply-to headers only).
    """

    #queues = {}
    #do_restore = False
    #supports_fanout = True

    def __init__(self, transport, connection):
        self.transport = transport
        self.connection = connection
        self.closed = False

    def prepare_message(self, message, *a, **kw):
        return message

    def exchange_declare(self, *a, **kw):
        # XXX 
        return NotImplemented

    def basic_publish(self, message, **kw):
        exchange = kw.get('exchange')
        routing_key = kw.get('routing_key')
        queue = kw.get('queue')
        if exchange and routing_key:
            dest = "/exchange/{}/{}".format(exchange, routing_key)
        elif queue:
            dest = "/queue/{}".format(queue)
        else:
            raise Exception('Not Implemented')

        conn = self.connection.get_connection()
        conn.send(destination=dest,
                  body=message,
                  headers=kw,
                  ack='auto',
                  persistent='true')
        print("basic_publish", self,  kw)


    def close(self):
        # doto
        if not self.closed:
            self.closed = True
            self.connection.close_channel(self)



class Connection(stomp.Connection):
    Channel = Channel

    def __init__(self, **connection_options):
        self.connection_options = connection_options
        self.channels = []

        self._conn = stomp.Connection(**connection_options)
        self._conn.start()
        self._conn.connect(wait=True) # todo: learn it

    def get_connection(self):
        return self._conn

    def close_channel(self, channel):
        try:
            self.channels.remove(channel)
        except ValueError:
            pass
        finally:
            channel.connection = None


class Transport(virtual.Transport):
    Channel = Channel
    Connection = Connection

    default_port = DEFAULT_PORT
    state = virtual.BrokerState()

    driver_name = 'stomp'
    driver_type = 'stomp'

    def __init__(self, client, **kwargs):
        if stomp is NA:
            raise ImportError('Missing stomp library (pip install stomp.py)')
        super(Transport, self).__init__(client, **kwargs)
        self.client = client
        self.default_port = kwargs.get('default_port') or self.default_port

    def establish_connection(self):
        conninfo = self.client

        for name, default_value in items(self.default_connection_params):
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)
        if conninfo.hostname == 'localhost':
            conninfo.hostname = '127.0.0.1'
 
        conn = self.Connection(host_and_ports=[(conninfo.hostname, conninfo.port)])
        conn.client = self.client
        conn.state = self.state
        return conn

    def close_connection(self, connection):
        connection.client = None
        connection.disconnect()

    def create_channel(self, connection):
        channel = connection.Channel(self, connection)
        connection.channels.append(channel)
        return channel

    @property
    def default_connection_params(self):
        return {'userid': 'guest', 'password': 'guest',
                'port': self.default_port,
                'hostname': 'localhost'}

    def driver_version(self):
        return stomp.__version__
