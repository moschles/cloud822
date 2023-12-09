'''
+ ACTIVITY + 

    Reducer worker code. 
    
    Performs the `Reduce` portion within MapReduce paradigm.
    Reduce will take split inverted maps and combine
    words which are the same, but appear in different books or buckets. 
    
    For example,  given ,

    "tiger" ---> { 
        (bookA,bucket6,3)
        (bookA,bucket15,1)
    }

    "tiger" ---> { 
        (bookC,bucket2,10)
        (bookD,bucket6,3)
    }

    produce

    "tiger" ---> { 
        (bookA,bucket6,3)
        (bookA,bucket15,1)
        (bookC,bucket2,10)
        (bookD,bucket6,3)
    }
    
    This would be performed over all unique words beginning with 't'. 
    Other parallel reducers target other letters than 't'. 

date: Wednesday, December 6, 2023 8:44:05 PM
'''
import logging
import json
from typing import List

def main(payload: List[str] ) -> str:

    rebox = json.loads( "["  +  payload[0] + "]" )
    targetws = rebox[0]   # targetws is a python List

    rebox = json.loads( "["  +  payload[1] + "]" )
    bigScanner = rebox[0] # bigScanner is the inverted indeces from all buckets from all books.

    ret = { }

    for w in targetws :
        ret[w] = [ ]  

    for k in bigScanner.keys() :
        miniIndex = bigScanner[k] 
        for w in targetws :
            # Assume python is searching through the keys of the miniIndex here, 
            if (w in miniIndex) :
                prevEntry = ret[w] 
                prevEntry.append( miniIndex[w] )
                ret[w] = prevEntry  
    return ( json.dumps(ret) )
