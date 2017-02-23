"""
Prints x megabytes of text to stdout per second.

"""
import sys, time, random, string

if __name__ == '__main__':
    x = float(sys.argv[1])
    t = int(sys.argv[2])

    s = string.lowercase + string.digits + string.uppercase
    while t:
        print ''.join(random.choice(s) for i in xrange(int(x * 1024 * 1024)))
        t -= 1
        time.sleep(1)
    sys.exit(0)
