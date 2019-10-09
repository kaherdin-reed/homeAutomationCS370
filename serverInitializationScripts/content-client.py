#!/usr/bin/env python3
import socket
import sys
import pika
import os


local_directory = ""


def get_remote_file(remote_host, remote_file_path, local_file_path):
    s = socket.socket()
    port = 48463
    s.connect((remote_host, port))
    s.send(remote_file_path.encode('utf-8'))
    f = open(local_file_path, 'wb')
    data = s.recv(4096)
    while data:
        f.write(data)
        data = s.recv(4096)
    f.close()
    s.close()


QUEUE_NAME = 'file_transfer'


def decode_message(ch, method, properties, body):
    remote_host, remote_path, filename = body.decode('utf-8').split(',')

    print(remote_host)
    print(remote_path)
    print(filename)

    if filename == '':
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    local_path = os.path.join(local_directory, filename)

    get_remote_file(remote_host, remote_path, local_path)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def listen_loop(connection):
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_consume(decode_message, queue=QUEUE_NAME)
    channel.start_consuming()


if __name__ == '__main__':
    rabbitmq = sys.argv[1]
    local_directory = sys.argv[2]
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq))
    listen_loop(connection)

