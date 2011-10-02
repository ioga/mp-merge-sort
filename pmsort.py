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
import argparse
import sys

BUF_SIZE = 8192
INPUT_FILE = 'input'
OUTPUT_FILE = 'output'
SORT_MEMORY_COUNT = 1 * 1024 * 1024

class Sorter(multiprocessing.Process):
    def __init__(self, filename, queue, tmpdir, sort_mem_count):
        self.filename = filename
        self.q = queue
        self.tmpdir = tmpdir
        self.sort_mem_count = sort_mem_count
        super(Sorter, self).__init__()

    def run(self):
        i = 0
        smc = self.sort_mem_count
        with open(self.filename, 'rb') as fin:
            while True:
                try:
                    data = numpy.fromfile(file=fin, dtype=numpy.uint32, count=smc)
                except MemoryError:
                    print 'Shrinking in-memory sort'
                    smc /= 2
                    continue
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
    QUEUE_TIMEOUT = 0.5
    def __init__(self, queue, lock, counter, maxcount, tmpdir, bufsize):
        self.q = queue
        self.l = lock
        self.c = counter
        self.m = maxcount
        self.tmpdir = tmpdir
        self.bs = bufsize
        super(Merger, self).__init__()
    
    def run(self):
        while self.c.value < self.m-1:
            with self.l:
                try: a = self.q.get(timeout=self.QUEUE_TIMEOUT)
                except Queue.Empty: continue
                try: b = self.q.get(timeout=self.QUEUE_TIMEOUT)
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
        write_count = self.bs / 4
        for chunk in iter(lambda: ''.join(struct.pack('I', item) for item in islice(iterable, write_count)), ''):
            f.write(chunk)

    def read_file(self, f):
        read_buf = self.bs
        for chunk in iter(lambda: f.read(read_buf), ''):
            for c in xrange(len(chunk)/4):
                yield struct.unpack('I', chunk[c*4:c*4+4])[0]

class SortRunner(object):
    DEFAULT_CPUS = 4

    def __init__(self, input, output, temp=None, cpus=None, bufsize=BUF_SIZE, sort_mem_count=SORT_MEMORY_COUNT):
        self.input, self.output = input, output
        if not self.check_input():
            raise AssertionError()

        self.tmpdir = tempfile.mkdtemp(dir=temp)        # Temporary directory for sorted parts
        self.queue = multiprocessing.Queue()            # Job queue
        self.lock = multiprocessing.Lock()              # Lock for queue.get synchronization
        self.counter = multiprocessing.Value('I', 0)    # Job counter
        self.chunks = self.get_chunks()
        self.cpus = cpus or self._cpu_count()           # Number of merge processes
        self.bs = bufsize
        self.sort_mem_count = sort_mem_count

    def run(self):
        self.sorter = Sorter(self.input, self.queue, self.tmpdir, self.sort_mem_count)
        self.sorter.start()
        self.pool = [Merger(self.queue, self.lock, self.counter, self.chunks, self.tmpdir, self.bs) for i in range(self.cpus)]
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

class SortValidator(object):
    def __init__(self, input, output, bufsize=BUF_SIZE):
        self.input, self.output, self.bs = input, output, bufsize
    def run(self):
        isize = os.path.getsize(INPUT_FILE)
        osize = os.path.getsize(OUTPUT_FILE)
        if isize != osize:
            raise AssertionError('File size differs. Found {0}, expected {1}'.format(osize, isize))
        with open(OUTPUT_FILE, 'rb') as f:
            last = 0
            i = 0
            bs = self.bs
            for chunk in iter(lambda: f.read(bs), ''):
                for c in xrange(len(chunk)/4):
                    curr = struct.unpack_from('I', chunk[c*4:c*4+4])[0]
                    if curr < last:
                        raise AssertionError('Error at position {0}, values {1},{2}'.format(i, last, curr))
                    i += 1
                    last = curr

def main():
    parser = argparse.ArgumentParser(description='Sort raw binary file as an array of uint32')
    parser.add_argument('input', metavar='INPUT', help='Input file name')
    parser.add_argument('output', metavar='OUTPUT', help='Output file name')
    parser.add_argument('-bs', help='Buffer size (defaults to {0}, must be multiple of 4)'.format(BUF_SIZE), metavar='BYTES', default=BUF_SIZE)
    parser.add_argument('-j', help='Number of workers (defaults to cpu_count)', metavar='JOBS')
    parser.add_argument('-smc', help='Number of elements to sort in memory (defaults to {0})'.format(SORT_MEMORY_COUNT), default=SORT_MEMORY_COUNT)
    parser.add_argument('-t', help='Temporary directory', metavar='TMPDIR')
    parser.add_argument('-c', help='Check output file validity', action='store_true')
    args = parser.parse_args()
    if args.c:
        SortValidator(args.input, args.output, args.bs).run()
        print 'OK'
        return
    if args.bs % 4 or not args.smc > 0 or args.j is not None and args.j < 0:
        sys.exit('Bad input parameter')
    SortRunner(args.input, args.output, args.t, args.j, args.bs, args.smc).run()

if __name__ == "__main__":
    main()
