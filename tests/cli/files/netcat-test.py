#!/usr/bin/env python3

import socket

TCP_IP = '0.0.0.0'
TCP_PORT = 5005
BUFFER_SIZE = 1024

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

while True:
    conn, addr = s.accept()
    data = conn.recv(BUFFER_SIZE)
    if not data:
        conn.close()
        continue
    if data == b"'yo dawg!'":
        conn.send(b'Hi this is dawg')
    else:
        conn.send(b'No, this is dawg')
    conn.close()
    break

s.close()
