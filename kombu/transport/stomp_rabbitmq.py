import stomp
print dir(stomp)

from kombu.five import items
from kombu.transport import base

DEFAULT_PORT = 61613

#class Connection(stomp.Connection):

class NA(object):
    pass


class Transport(base.Transport):
    default_port = DEFAULT_PORT

    driver_name = 'stomp'
    driver_type = 'stomp'

    def __init__(self, client, **kwargs):
        self.client = client
        self.default_port = kwargs.get('default_port') or self.default_port

        if stomp is NA:
            raise ImportError('Missing stomp library (pip install stomp.py)')

    def establish_connection(self):
        """Establish connection to the STOMP server."""
        conninfo = self.client

        for name, default_value in items(self.default_connection_params):
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)
        if conninfo.hostname == 'localhost':
            conninfo.hostname = '127.0.0.1'
 
        conn = stomp.Connection(host_and_ports=[(conninfo.hostname, conninfo.port)])
        conn.start()
        conn.connect(wait=True) # todo: learn it
        conn.client = self.client
        return conn

    def close_connection(self, connection):
        """Close the STOMP connection."""
        connection.client = None
        connection.disconnect()

    def create_channel(self, connection):
        return connection.channel()


    @property
    def default_connection_params(self):
        return {'userid': 'guest', 'password': 'guest',
                'port': self.default_port,
                'hostname': 'localhost'}
