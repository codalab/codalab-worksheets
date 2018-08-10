#!/usr/bin/env python

import socket
import time


TCP_IP = '0.0.0.0'
TCP_PORT = 5005
BUFFER_SIZE = 1024

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
while 1:
    # print "polling data"
    conn, addr = s.accept()
    # print 'Connection address:', addr
    data = conn.recv(BUFFER_SIZE)
    if not data:
        conn.close()
        continue
    # print "received data:", data
    if data != "yo dawg!":
        conn.send("No, this is dawg")  # echo
    else:
        conn.send("Hi this is dawg")
    conn.close()
    break
    time.sleep(1)
s.close()
