#!/usr/bin/env python

import struct
import numpy
#import array
import os
from collections import deque
from heapq import merge
import shutil
from itertools import islice

import tempfile

BUF_SIZE = 8192
READ_BUF = BUF_SIZE
WRITE_BUF_NUM = BUF_SIZE/4
INPUT_FILE = 'input'
OUTPUT_FILE = 'output'
SORT_MEMORY_COUNT = 1000000

def sort_in_memory(f, count):
    data = numpy.fromfile(file=f, dtype=numpy.uint32, count=count)
    data.sort()
    return data
#    with open(OUTPUT_FILE, 'wb') as fout:
#        data.tofile(fout)

def read_file(f):
    for chunk in iter(lambda: f.read(READ_BUF), ''):
        for c in xrange(len(chunk)/4):
            yield struct.unpack('I', chunk[c*4:c*4+4])[0]

def write_file(f, iterable):
    for chunk in iter(lambda: ''.join(struct.pack('I', item) for item in islice(iterable, WRITE_BUF_NUM)), ''):
        f.write(chunk)
#    for item in iterable:
#        f.write(struct.pack('I', item))


def merge_files(tmpdir, files):
    while len(files) > 1:
        print len(files)
        with open(files.popleft(), 'rb') as f1, open(files.popleft(), 'rb') as f2:
            fd, pathname = tempfile.mkstemp(dir=tmpdir)
            with os.fdopen(fd, 'wb') as fout:
                write_file(fout, merge(read_file(f1), read_file(f2)))
            files.append(pathname)
    return files.pop()

def main():
    tmpdir = tempfile.mkdtemp()
    files = deque()
    # In memory stage
    with open(INPUT_FILE, 'rb') as fin:
        while True:
            data = sort_in_memory(fin, SORT_MEMORY_COUNT)
            if not len(data):
                break
            fd, pathname = tempfile.mkstemp(dir=tmpdir)
            with os.fdopen(fd, 'wb') as fout:
                data.tofile(fout)
            files.append(pathname)
    # Merge stage
    final = merge_files(tmpdir, files)
    shutil.move(final, OUTPUT_FILE)
    shutil.rmtree(tmpdir)

if __name__ == "__main__":
    main()
