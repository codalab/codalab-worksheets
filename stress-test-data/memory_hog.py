import sys, time

MEGA = 1024 ** 2
MEGA_STR = ' ' * MEGA

if __name__ == '__main__':
    n = int(sys.argv[1])
    t = int(sys.argv[2])
    ar = []
    for i in range(n):
        try:
            ar.append(MEGA_STR + str(i))
        except MemoryError:
            break

    while t:
        time.sleep(1)
        print "I'm hoggin'"
        t -= 1
