#!/usr/bin/env python3
import socket
import sys
from collections import deque
import logging

import pika
import pyinotify

flags = pyinotify.EventsCodes.FLAG_COLLECTIONS['OP_FLAGS']
LOGGER = logging.getLogger(__name__)


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))  # this will fail
        ip = s.getsockname()[0]
    except:
        ip = -1
    finally:
        s.close()
    return ip


class Publisher(object):
    EXCHANGE = ''
    EXCHANGE_TYPE = 'direct'
    PUBLISH_INTERVAL = 0.5
    QUEUE_NAME = 'file_transfer'

    def __init__(self, connection_parameters):
        self._connection = None
        self._channel = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        self._connection_parameters = connection_parameters
        self._closing = False
        self._waiting = deque()

    def connect(self):
        LOGGER.info("Connecting to server")
        return pika.SelectConnection(self._connection_parameters, self.on_connection_open, stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        LOGGER.info("Connection opened")
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        LOGGER.info("Adding connection close callback")
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning("Connection closed, trying again in 5 seconds (%s) %s", reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self._connection.ioloop.stop()

        self._connection = self.connect()
        self._connection.ioloop.start()

    def open_channel(self):
        LOGGER.info("Creating channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info("Channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_queue(self.QUEUE_NAME)

    def add_on_channel_close_callback(self):
        LOGGER.info("Adding channel close callback")
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        LOGGER.warning("Channel was closed: (%s) %s", reply_code, reply_text)
        if not self._closing:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        LOGGER.info("Declaring exchange %s", exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        LOGGER.info("Exchange declared")
        self.setup_queue(self.QUEUE_NAME)

    def setup_queue(self, queue_name):
        LOGGER.info("Declaring queue %s", queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        # here you would bind the queue to a routing key if not using default exchange
        LOGGER.info("Queue declared")
        self.start_publishing()
        return

    def on_bindok(self, unused_frame):
        # callback for queue_bind
        # here is where you would call start_publishing if not using default exchange
        return

    def start_publishing(self):
        LOGGER.info("Issuing consumer related RPC commands")
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        LOGGER.info("Issuing Confirm.Select RPC command")
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.info("Received %s for delivery tag: %i",
                    confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1

        self._deliveries.remove(method_frame.method.delivery_tag)

    def schedule_next_message(self):
        if self._stopping:
            return
        LOGGER.info("Scheduling next message for %0.1f seconds",
                    self.PUBLISH_INTERVAL)

        self._connection.add_timeout(self.PUBLISH_INTERVAL,
                                     self.publish_message)

    def publish_message(self):
        if self._stopping:
            return
        if not self._waiting:
            self.schedule_next_message()
            return

        message = self._waiting.popleft()
        properties = pika.BasicProperties(app_id='fs-watcher',
                                          content_type='text/plain')

        self._channel.basic_publish(self.EXCHANGE, self.QUEUE_NAME,
                                    body=message, properties=properties)

        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.info("Published message # %i", self._message_number)
        self.schedule_next_message()

    def close_channel(self):
        LOGGER.info("Closing channel")
        if self._channel:
            self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        LOGGER.info("Stopping")
        self._stopping = True
        self.close_channel()
        self.close_connection()
        self._connection.ioloop.start()  # allow clean disconnection from server
        LOGGER.info("Stopped")

    def close_connection(self):
        LOGGER.info("Closing connection")
        self._closing = True
        self._connection.close()

    def send_message(self, message):
        self._waiting.append(message)


class Watcher(pyinotify.ProcessEvent):
    def __init__(self, publisher):
        super().__init__()

        self._node_ip = get_ip()
        if self._node_ip == -1:
            print("Unable to determine local IP address")
            exit(1)

        self._publisher = publisher
        self._sent_files = []
        self._mask = flags['IN_CLOSE_WRITE'] | \
                     flags['IN_CLOSE_NOWRITE'] | \
                     flags['IN_MOVED_TO']

    def construct_message(self, local_path, filename):
        return str(self._node_ip) + ',' + local_path + ',' + filename

    def process_default(self, event):
        if self._mask & event.mask == 0:
            return

        if event.name == '':
            return

        if event.name in self._sent_files:
            return

        if '.jpg' not in event.name:
            return

        self._publisher.send_message(self.construct_message(event.pathname, event.name))
        self._sent_files.append(event.name)

        print(event)
        print("Sent " + self.construct_message(event.pathname, event.name))
        print('================')


def start_watching(directory, rabbitmq_host):
    logging.basicConfig(level=logging.WARNING)
    credentials = pika.PlainCredentials('test', 'test')
    connection_parameters = pika.ConnectionParameters(rabbitmq_host, 5672, '/', credentials,
                                                      heartbeat_interval=10)

    publisher = Publisher(connection_parameters)

    watcher = Watcher(publisher)

    wm = pyinotify.WatchManager()

    notifier = pyinotify.ThreadedNotifier(wm, default_proc_fun=watcher)
    notifier.start()

    wm.add_watch(directory, pyinotify.ALL_EVENTS)

    try:
        publisher.run()
    except KeyboardInterrupt:
        notifier.stop()
        publisher.stop()


if __name__ == '__main__':
    start_watching(sys.argv[1], sys.argv[2])
