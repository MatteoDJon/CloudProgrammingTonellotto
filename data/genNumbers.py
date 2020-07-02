import numpy
import sys
rng = numpy.random.default_rng()
rows = int(sys.argv[1])
columns = int(sys.argv[2])
matrx = 2 * rng.random((rows, columns)) - 1
for arr in matrx:
  print(' '.join(map(str,arr)))
