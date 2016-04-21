import sys
import collections
import stomp

from array import array

from . import virtual
from kombu.five import items, Empty
from kombu.transport import base
from kombu.utils.scheduling import FairCycle
from kombu.utils import emergency_dump_state, uuid
"""
TODO Agenda:
- Declare
- Use base.Channel insted virtual.Channel (remove poller)
- Receive messages from queue on startup
- Determine and pass correct headers
- Determine Channel from subscriber ID
- All exchange types (for now only direct)
- Code refactoring
"""

DEFAULT_PORT = 61613

ARRAY_TYPE_H = 'H' if sys.version_info[0] == 3 else b'H'

class NA(object):
    pass

class StompListener(stomp.ConnectionListener):
    def __init__(self, transport):
        self.transport = transport

    def on_error(self, headers, message):
        print('received an error "%self"' % message)

    def on_message(self, headers, body):
        # DOTO raplace with no pooler
        headers['delivery_tag'] = headers['message-id']

        if headers['destination'].startswith('/exchange/'):
            exchange, routing_key = headers['destination'].split("/")[2:4]

            headers['delivery_info'] = dict(
                exchange=exchange,
                routing_key=routing_key, 
                )

        self.transport.messages.append(
            {'body':body, 'properties':headers})

class Channel(virtual.Channel):
    """
    The RabbitMQ STOMP adapter supports a number of different destination types:

    /exchange -- SEND to arbitrary routing keys and SUBSCRIBE to arbitrary binding patterns;
    /queue -- SEND and SUBSCRIBE to queues managed by the STOMP gateway;
    /amq/queue -- SEND and SUBSCRIBE to queues created outside the STOMP gateway;
    /topic -- SEND and SUBSCRIBE to transient and durable topics;
    /temp-queue/ -- create temporary queues (in reply-to headers only).
    """
    def __init__(self, transport, connection):
        super(Channel, self).__init__(connection)
        self.transport = transport
        self.connection = connection
        
        self.closed = False
        self._tag_to_queue = {}
        self._active_queues = []
        self._consumers = set()

    def _get(self, queue, timeout=None):
        # XXX
        try:
            message = self.transport.messages.popleft()
        except IndexError:
            raise Empty
        else:
            return message

    def _put(self, routing_key, message, exchange=None, **kwargs):
    	print(routing_key, message, exchange, kwargs)
        message['properties'].pop('message-id', None)
        exchange = message['properties']['delivery_info'].get('exchange')
        routing_key = message['properties']['delivery_info'].get('routing_key')
        queue = kwargs.get('queue')
        if exchange and routing_key:
            dest = "/exchange/{}/{}".format(exchange, routing_key)
        elif queue:
            dest = "/queue/{}".format(queue)
        else:
            raise Exception('Not Implemented')

        conn = self.connection
        conn.send(destination=dest,
                  body=message['body'],
                  headers=message['properties'],
                  ack='auto',
                  persistent='true')
        

    def prepare_message(self, body, priority=None, content_type=None,
                        content_encoding=None, headers=None, properties=None):
        properties = properties or {}
        info = properties.setdefault('delivery_info', {})
        info['priority'] = priority or 0

        return {
            'body': body,
            'content-encoding': content_encoding,
            'content-type': content_type,
            'headers': headers or {},
            'properties': properties or {},
        }

    def queue_declare(self, queue, passive=False, durable=False,
                      exclusive=False, auto_delete=True, nowait=False,
                      arguments=None):
        headers = {'durable': durable,
                   'exclusive': exclusive,
                   'auto-delete': auto_delete,
                   'arguments': arguments}

        subscribe_id = uuid()
        self.connection.subscribe(
            destination='/queue/{}'.format(queue),
            id=subscribe_id,
            headers=headers
            )
        self.connection.unsubscribe(
            destination='/queue/{}'.format(queue),
            id=subscribe_id,
            headers=headers
            )

    def queue_bind(self, queue, exchange=None, routing_key='',
                   arguments=None, **kwargs):
        exchange = exchange or 'amq.direct'
        if self.state.has_binding(queue, exchange, routing_key):
            return
        # Add binding:
        self.state.binding_declare(queue, exchange, routing_key, arguments)
        # Update exchange's routing table:
        table = self.state.exchanges[exchange].setdefault('table', [])
        
        meta = self.typeof(exchange).prepare_bind(
            queue, exchange, routing_key, arguments,
        )
        table.append(meta)
        #if self.supports_fanout:
        #    self._queue_bind(exchange, *meta)

        if exchange and routing_key:
            dest = "/exchange/{}/{}".format(exchange, routing_key)
        elif queue:
            dest = "/queue/{}".format(queue)
        else:
            raise Exception('Not Implemented')

        conn = self.connection
        conn.subscribe(destination=dest, id=uuid(), headers={}, ack='auto')

    
    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        self._tag_to_queue[consumer_tag] = queue
        self._active_queues.append(queue)

        def _callback(raw_message):
            message = self.Message(self, raw_message)
            if not no_ack:
                self.qos.append(message, message.delivery_tag)
            return callback(message)

        self.transport._callbacks[queue] = _callback
        self._consumers.add(consumer_tag)
        self._reset_cycle()


    def basic_publish(self, message, exchange, routing_key, **kwargs):
    	print(message, kwargs)
        dest = "/exchange/{}/{}".format(exchange, routing_key)

        self.connection.send(destination=dest,
                             body=message['body'],
                             headers=message['headers'])

    def close(self):
        # doto
        if not self.closed:
            self.closed = True
            self.connection.close_channel(self)



class Connection(stomp.Connection):
    Channel = Channel

    def __init__(self, transport, **connection_options):
        self.channels = []
        self._callbacks = {}

        self._avail_channels = []
        self._avail_channel_ids = array(
            ARRAY_TYPE_H, range(65535, 0, -1),
        )

        super(Connection, self).__init__(**connection_options)

    def get_connection(self):
        return self

    def close_channel(self, channel):
        try:
            self.channels.remove(channel)
        except ValueError:
            pass
        finally:
            channel.connection = None

    def channel(self, channel_id=None):
        try:
            return self.channels[channel_id]
        except KeyError:
            return Channel(self, channel_id)


class Transport(virtual.Transport):
    Channel = Channel
    Connection = Connection
    Cycle = FairCycle

    default_port = DEFAULT_PORT
    state = virtual.BrokerState()

    driver_name = 'stomp'
    driver_type = 'stomp'

    #: active channels.
    channels = None

    def __init__(self, client, **kwargs):
        if stomp is NA:
            raise ImportError('Missing stomp library (pip install stomp.py)')
        super(Transport, self).__init__(client, **kwargs)
        self.client = client
        self.default_port = kwargs.get('default_port') or self.default_port
        self.messages = collections.deque([])

    def establish_connection(self):
        conninfo = self.client

        for name, default_value in items(self.default_connection_params):
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)
        if conninfo.hostname == 'localhost':
            conninfo.hostname = '127.0.0.1'
 
        conn = self.Connection(
            transport=self,
            host_and_ports=[(conninfo.hostname, conninfo.port)])
        conn.set_listener('', StompListener(self))
        conn.start()
        conn.connect(wait=True)
        conn.client = self.client
        conn.state = self.state
        return conn


    def close_connection(self, connection):
        connection.client = None
        connection.disconnect()

    def create_channel(self, connection):
        channel = connection.Channel(self, connection)
        self.channels.append(channel)
        return channel

    @property
    def default_connection_params(self):
        return {'userid': 'guest', 'password': 'guest',
                'port': self.default_port,
                'hostname': 'localhost'}

    def driver_version(self):
        return stomp.__version__
