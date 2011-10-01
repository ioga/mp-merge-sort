import struct
import os

INPUT_FILE = 'input'
OUTPUT_FILE = 'output'
READ_BUF = 8192

def check_sorted(f):
    last = 0
    i = 0
    for chunk in iter(lambda: f.read(READ_BUF), ''):#READ_BUF):
        for c in xrange(len(chunk)/4):
            curr = struct.unpack_from('I', chunk[c*4:c*4+4])[0]
            if curr < last:
                raise AssertionError('Error at position {0}, values {1},{2}'.format(i, last, curr))
            i += 1
            last = curr

def main():
    isize = os.path.getsize(INPUT_FILE)
    osize = os.path.getsize(OUTPUT_FILE)
    if isize != osize:
        raise AssertionError('File size differs. Found {0}, expected {1}'.format(osize, isize))
    with open(OUTPUT_FILE, 'rb') as f:
        check_sorted(f)
    print 'OK'

if __name__ == "__main__":
    main()
