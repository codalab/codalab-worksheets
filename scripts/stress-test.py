# Program to stress test time/memory/disk.
import argparse
import time
import os

parser = argparse.ArgumentParser()
parser.add_argument('-t', '--time', type=int, default=10, help='Number of seconds to run')
parser.add_argument('-m', '--memory', type=int, default=1024, help='Number of megabytes to allocate')
parser.add_argument('-d', '--disk', type=int, default=1024, help='Number of megabytes to write')
args = parser.parse_args()

bytes_in_mb = 1024 * 1024

mem_buf = ''
bytes_written = 0

for t in range(args.time):
    print 'time = %s, mem = %s MB, disk = %s MB' % \
        (t, len(mem_buf) / bytes_in_mb, bytes_written / bytes_in_mb)

    # Allocate memory
    new_mem_bytes = int(bytes_in_mb * args.memory / args.time)
    mem_buf += '*' * new_mem_bytes 

    # Write to disk
    new_disk_bytes = int(bytes_in_mb * args.disk / args.time)
    bytes_written += new_disk_bytes
    os.system('dd if=/dev/zero of=output bs=%s count=1 oflag=append conv=notrunc' % new_disk_bytes)

    time.sleep(1)
