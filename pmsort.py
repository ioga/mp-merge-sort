#!/usr/bin/env python

import struct
import numpy
#import array
import os
from collections import deque
from heapq import merge
import shutil
from itertools import islice
from multiprocessing import Value, cpu_count, Lock, Process
import multiprocessing
import tempfile
import Queue

BUF_SIZE = 8192
READ_BUF = BUF_SIZE
WRITE_BUF_NUM = BUF_SIZE/4
INPUT_FILE = 'input'
OUTPUT_FILE = 'output'
SORT_MEMORY_COUNT = 16 * 1024 * 1024

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


def merge_two_files(tmpdir, file1, file2):
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        fd, pathname = tempfile.mkstemp(dir=tmpdir)
        with os.fdopen(fd, 'wb') as fout:
            write_file(fout, merge(read_file(f1), read_file(f2)))
        return pathname

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

def worker(work_queue, l, c, tmpdir):
    print 'Worker started'
    pass

class Sorter(Process):
    def __init__(self, filename, queue, tmpdir):
        self.filename = filename
        self.q = queue
        self.tmpdir = tmpdir
        super(Sorter, self).__init__()

    def run(self):
        i = 0
        with open(self.filename, 'rb') as fin:
            while True:
                data = sort_in_memory(fin, SORT_MEMORY_COUNT)
                if not len(data):
                    break
                fd, pathname = tempfile.mkstemp(dir=self.tmpdir)
                with os.fdopen(fd, 'wb') as fout:
                    data.tofile(fout)
                self.q.put(pathname)
                i += 1
                print 'Sorted chunk {0}'.format(i)

class Merger(Process):
    def __init__(self, queue, lock, counter, maxcount, tmpdir):
        self.q = queue
        self.l = lock
        self.c = counter
        self.m = maxcount
        self.tmpdir = tmpdir
        super(Merger, self).__init__()
    def run(self):
        print 'Running Merger'
        while self.c.value < self.m-1:
            #print self.c.value, self.m
            self.l.acquire()
            try: a = self.q.get(timeout=0.1)
            except Queue.Empty:
                self.l.release()
                continue
            try: b = self.q.get(timeout=0.1)
            except Queue.Empty:
                self.q.put(a)
                self.l.release()
                continue
            self.l.release()

            print 'Starting merge %s' % os.getpid()
            new = merge_two_files(self.tmpdir, a, b)
            print 'Ending merge %s' % os.getpid()
            self.q.put(new)
            self.c.value += 1
            print self.c.value

def main_par():
    tmpdir = tempfile.mkdtemp()
    files = multiprocessing.Queue()
    counter = Value('I', 0)
    lock = Lock()
    try: cpus = cpu_count()
    except NotImplementedError: cpus = 4
    size = os.path.getsize(INPUT_FILE)
    if size % 4:
        raise AssertionError('Incorrect input')
    maxcount = size / 4 / SORT_MEMORY_COUNT + (1 if size / 4 % SORT_MEMORY_COUNT else 0)
    s = Sorter(INPUT_FILE, files, tmpdir)
    s.start()
    pool = [Merger(files, lock, counter, maxcount, tmpdir) for i in range(cpus)]
    map(lambda p: p.start(), pool)
    s.join()
    print 'Finished sort step'
    for p in pool:
        p.join()
    print files.qsize()
    final = files.get(block=False)
    shutil.move(final, OUTPUT_FILE)
    shutil.rmtree(tmpdir)

if __name__ == "__main__":
    main_par()
