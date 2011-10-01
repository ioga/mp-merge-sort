#!/usr/bin/env python

import struct
import numpy
import array

READ_BUF = 8192
INPUT_FILE = 'input'
OUTPUT_FILE = 'output'

def main():
    fin = open(INPUT_FILE, 'rb')
    for chunk in fin.read(READ_BUF):
        pass

def main_np():
    with open(INPUT_FILE, 'rb') as fin:
        data = numpy.fromfile(file=fin, dtype=numpy.uint32)
    data.sort()
    with open(OUTPUT_FILE, 'wb') as fout:
        data.tofile(fout)

def main_array():
    data = array.array('I')
    with open(INPUT_FILE, 'rb') as fin:
        try: data.fromfile(fin, 4194304)
        except EOFError: pass
    data = array.array('I', sorted(data))
    with open(OUTPUT_FILE, 'wb') as fout:
        data.tofile(fout)

def main_via_struct():
    fin = open(INPUT_FILE, 'rb')
    data = []
    for chunk in iter(lambda: fin.read(4), ''):#READ_BUF):
        data.append(struct.unpack('I', chunk)[0])
    with open(OUTPUT_FILE, 'wb') as fout:
        data.sort()
        for item in data:
            fout.write(struct.pack('I', item))

if __name__ == "__main__":
    from timeit import Timer
    t1 = Timer("main_np()", "from __main__ import main_np")
    t2 = Timer("main_array()", "from __main__ import main_array")
    t3 = Timer("main_via_struct()", "from __main__ import main_via_struct")
    n = 10
    #print t3.timeit(number=n)
    print t1.timeit(number=n)
    #print t2.timeit(number=n)
