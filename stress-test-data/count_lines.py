import sys, time

if __name__ == '__main__':
    f = sys.argv[1]
    t = int(sys.argv[2])

    while t:
        with open(f, 'r') as fd:
            content = fd.readlines()
            count = len(content)
        print 'count:', count
        t -= 1
        time.sleep(1)
    sys.exit(0)
