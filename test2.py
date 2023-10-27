import websockets
from websockets.sync.client import connect
import time
import json

try:
    with websockets.sync.client.connect("ws://ws-server:2901/send/Andrew-Gs-MBP-6.lan") as ws:
        ws.send("giberrish")
        ws.send(json.dumps({'hi': 1}))
        ack = ws.recv()
except Exception as e:
    import pdb

    pdb.set_trace()
    print(e)
    print("Exception")
time.sleep(3)
