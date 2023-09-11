# -*- coding: utf-8 -*-
"""
post-processing for project0 

date : Sunday, September 10, 2023 9:14:59 PM
"""

import numpy as np
import sys
import collections
import math

## ## ##
def percentile( nth , nparr, verbose = False ): 
    sz = float( nparr.size ) 
    fnth = float(nth)
    midperc = (fnth/100.0) * (sz+1.0)
    lowperc = int( math.floor(midperc) )
    hiperc = int( math.ceil(midperc) )
    assert( not lowperc == hiperc )
    vl = nparr[lowperc-1]
    vh = nparr[hiperc-1]
    if verbose :
        print( "{}th percentile".format( fnth ) )
        print( "used ", vl, " at pos ", lowperc-1 )
        print( "used ", vh, " at pos ", hiperc-1 )
    return(  0.5 * (vl+vh)  )

# read the results file
try:
    fh = open("results10000", 'r')
except IOError as e:
    print(e)
    sys.exit(-1)
else:
    lines = fh.readlines()
    fh.close()
    
 
# initializing deque
de_timeouts = collections.deque( [] )
de_nans = collections.deque( [] )
de_goods = collections.deque( [] )

total = len(lines)
for i in range( total ) :
    if (lines[i].find('a')) > (-1):
        de_nans.append(lines[i])
    elif  (lines[i].find('-')) > (-1):
        de_timeouts.append(lines[i])
    else :
        de_goods.append(lines[i])

print("timeouts ",  len(de_timeouts))
print("packets dropped ",  len(de_nans))
print("successful ",  len(de_goods))

rtts = np.zeros(shape=(len(de_goods)), dtype=np.float64 )
i=0
for elem in de_goods:
    ct = elem.find(' ')
    rtts[i] = float( elem[ct:] ) 
    i=1+i
    
rttsrank = np.sort( rtts )
print( rttsrank[0] )
print( rttsrank[9999] )

perc99 = percentile(99.0, rttsrank , verbose=True)
perc99p9 = percentile(99.9, rttsrank , verbose=True)

print("99th percentile", perc99 )
print("99.9th percentile", perc99p9 )


rttsrev = -np.sort( -rtts  )

perc99 = percentile(99.0, rttsrev , verbose=True)
perc99p9 = percentile(99.9, rttsrev , verbose=True)

print("fast 99th percentile", perc99 )
print("fast 99.9th percentile", perc99p9 )

print( "median" ,  np.median( rttsrank ) )
print( "average" ,  np.average( rttsrank ) )
print("Done.")
