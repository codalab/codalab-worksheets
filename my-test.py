from scripts.test_util import Timer
import time

with Timer(timeout_seconds=3):
  while True:
    print("hello")
    time.sleep(0.5)
