#!/usr/bin/env python3
import socket

s = socket.socket()
host = '0.0.0.0'
port = 48463
s.bind((host, port))

s.listen()
while True:
    c, addr = s.accept()
    print("Connection from: " + str(addr))
    filename = c.recv(4096).decode()
    if filename == '':
        c.close()
        continue
    print("Requested file " + filename)
    f = open(filename, 'rb')
    data = f.read(4096)
    while data:
        c.send(data)
        data = f.read(4096)
    f.close()
    c.close()

