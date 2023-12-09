'''
+ ACTIVITY + 

    Mapper worker code. 
    
    Performs the `Map` portion within MapReduce paradigm.
    Mapper will convert a single bucket into an inverted index. 
    
    "word1" ---> { 
        (title,bucket no.,frequency)
        (title,bucket no.,frequency)
        (title,bucket no.,frequency)
            .
            .
            .
        }  

    "word2" ---> { 
        (title,bucket no.,frequency)
        (title,bucket no.,frequency)
        (title,bucket no.,frequency)
            .
            .
            .
        }

    Frequency is the count of occurances of a term. 

date: Wednesday, December 6, 2023 6:06:59 PM
'''

import logging
import asyncio
import json
import pandas as pd
import numpy as np


async def main(payload:str) -> str:
    rebox = json.loads( "["  +  payload + "]" )
    recreate = rebox[0] 
    bucket = recreate 
    coln = 'F'
    # peel off book title  metadata
    title = bucket[-2]
    # peel off bucket number metadata
    bnum = bucket[-1] 
    bucket = bucket[:(-2)]
    uniques = sorted(set(bucket)) 
    framerows = len(uniques)
    df = pd.DataFrame( np.zeros((framerows,), dtype=int) , index=uniques, columns=[coln] )
    for w in bucket :
        c = df.at[w,coln]
        df.at[w,coln] = 1+c
    inverted = { }
    for w in uniques :
        inverted[w] = ( title , bnum , int(df.at[w,coln]) ) 
    return( json.dumps(inverted)  )

