#!/usr/bin/env python

import struct
import numpy
import os
import heapq
import shutil
from itertools import islice
import multiprocessing
import tempfile
import Queue

BUF_SIZE = 8192
READ_BUF = BUF_SIZE
WRITE_BUF_NUM = BUF_SIZE/4
INPUT_FILE = 'input'
OUTPUT_FILE = 'output'
SORT_MEMORY_COUNT = 1 * 1024 * 1024
QUEUE_TIMEOUT = 0.2

class Sorter(multiprocessing.Process):
    def __init__(self, filename, queue, tmpdir):
        self.filename = filename
        self.q = queue
        self.tmpdir = tmpdir
        super(Sorter, self).__init__()

    def run(self):
        i = 0
        with open(self.filename, 'rb') as fin:
            while True:
                data = numpy.fromfile(file=fin, dtype=numpy.uint32, count=SORT_MEMORY_COUNT)
                if not len(data):
                    break
                data.sort()
                fd, pathname = tempfile.mkstemp(dir=self.tmpdir)
                with os.fdopen(fd, 'wb') as fout:
                    data.tofile(fout)
                self.q.put(pathname)
                i += 1
                print 'Sorted chunk {0}'.format(i)

class Merger(multiprocessing.Process):
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
            with self.l:
                try: a = self.q.get(timeout=QUEUE_TIMEOUT)
                except Queue.Empty: continue
                try: b = self.q.get(timeout=QUEUE_TIMEOUT)
                except Queue.Empty:
                    self.q.put(a)   # Put it back
                    continue

            print 'Starting merge %s' % os.getpid()
            new = self.merge(a, b)
            os.unlink(a)
            os.unlink(b)
            print 'Ending merge %s' % os.getpid()
            self.q.put(new)
            self.c.value += 1
            print self.c.value

    def merge(self, file1, file2):
        with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
            fd, pathname = tempfile.mkstemp(dir=self.tmpdir)
            with os.fdopen(fd, 'wb') as fout:
                self.write_file(fout, heapq.merge(self.read_file(f1), self.read_file(f2)))
        return pathname

    def write_file(self, f, iterable):
        for chunk in iter(lambda: ''.join(struct.pack('I', item) for item in islice(iterable, WRITE_BUF_NUM)), ''):
            f.write(chunk)

    def read_file(self, f):
        for chunk in iter(lambda: f.read(READ_BUF), ''):
            for c in xrange(len(chunk)/4):
                yield struct.unpack('I', chunk[c*4:c*4+4])[0]

class SortRunner(object):
    DEFAULT_CPUS = 4

    def __init__(self, input, output, temp=None, cpus=None):
        self.input, self.output = input, output
        if not self.check_input():
            raise AssertionError()

        self.tmpdir = tempfile.mkdtemp(dir=temp)        # Temporary directory for sorted parts
        self.queue = multiprocessing.Queue()            # Job queue
        self.lock = multiprocessing.Lock()              # Lock for queue.get synchronization
        self.counter = multiprocessing.Value('I', 0)    # Job counter
        self.chunks = self.get_chunks()
        self.cpus = cpus or self._cpu_count()           # Number of merge processes


    def run(self):
        self.sorter = Sorter(self.input, self.queue, self.tmpdir)
        self.sorter.start()
        self.pool = [Merger(self.queue, self.lock, self.counter, self.chunks, self.tmpdir) for i in range(self.cpus)]
        map(lambda p: p.start(), self.pool)
        self.sorter.join()
        for p in self.pool:
            p.join()
        final = self.queue.get(block=False) # TODO
        shutil.move(final, self.output)
        shutil.rmtree(self.tmpdir)

    def _cpu_count(self):
        """Returns number of cpus using multiprocessing with default fallback"""
        try: return multiprocessing.cpu_count()
        except NotImplementedError: return self.DEFAULT_CPUS

    def check_input(self):
        return not os.path.getsize(self.input) % 4

    def get_chunks(self):
        return (os.path.getsize(self.input) / 4 + SORT_MEMORY_COUNT - 1 )/ SORT_MEMORY_COUNT

def main():
    SortRunner(INPUT_FILE, OUTPUT_FILE).run()

if __name__ == "__main__":
    main()
