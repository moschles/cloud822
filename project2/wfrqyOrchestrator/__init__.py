'''
+ ORCHESTRATOR + 

    wfrqyOrchestrator 
    
    Compile word frequency and apperance statistics in 10 books.
    Follow MapReduce paradigm.
    
    "word1" ---> { 
        (author,bucket no.,frequency)
        (author,bucket no.,frequency)
        (author,bucket no.,frequency)
            .
            .
            .
        }  

    "word2" ---> { 
        (author,bucket no.,frequency)
        (author,bucket no.,frequency)
        (author,bucket no.,frequency)
            .
            .
            .
        }

    Frequency is the count of occurances of a term in a bucket.

date:   
Thursday, December 7, 2023 10:51:09 PM
'''
import azure.functions as func
import azure.durable_functions as df
import logging
import json
import math
from time import perf_counter_ns

verboseStats = True

bookAuthors = { 
    "How Jerusalem Was Won" : "Massey",
    "Ukraine, the land and its people"  : "Rudnitsky" ,
    "Early Travels in Palestine": "Wright",
    "Mrs. Dalloway" : "Woolf"  ,
    "Origin of the Species": "Darwin",
    "The Professor" : "Bronte" ,
    "The Religion of the Samurai": "Nukariya",
    "The City of God" : "Augustine",
     "The Conquest of Bread" : "Kropotkin",
     "Creative Evolution" : "Bergson",
	 "The Story of Sigurd" : "Morris" 
}

ddos="DDOS Flood detected. Durable function halted. Reduce the number of books or reduce book length."

########
#   chatter 
########
def chatter( ch ) :
    if( verboseStats ) :
        logging.info(ch) 


########
#   rounded_div() 
########
def rounded_div( nm, dn ) :
    spanf = float(nm) /  float(dn)
    fp = spanf - math.floor(spanf) 
    if( fp > 0.4999 ) :
        return( 1+  (nm//dn) )
    else :
        return( nm//dn )

'''
LOAD BALANCER for  fixed number of Reducers. 
'''
def balanced_slicer( universe , slices ) :
    assert( slices < len(universe) )
    if( len(universe) < 2 ) :
        return( { 0 : universe[0] } )
    allTargets = { } 
    span = rounded_div( len(universe) , slices )
    if (span < 2) :
        k=0
        cur = [ ]
        for u in range(len(universe)) :
            cur.append(universe[u])
            if( len(cur) > 1 ) :
                allTargets[k] = cur.copy()
                cur = [ ]
                k=1+k
        return( allTargets )
        
    else :     
        big=0
        for sl in range(15+slices) :
            lslice = min(     sl*span , len(universe) ) 
            rslice = min( (sl+1)*span , len(universe) ) 
            targets = universe[lslice:rslice]
            if (not targets) :
                continue
            else :
                allTargets[sl] = targets
                big=sl
        firstpass =  len(list(allTargets.keys()))   
        secondpass = firstpass
        if (firstpass > slices) :
            while( secondpass > slices ) : 
                distr = (allTargets[big-1]) + (allTargets[big]).copy()
                allTargets[big-1] = distr 
                del allTargets[big]
                secondpass = len(list(allTargets.keys()))
                big=big-1
        assert( secondpass <= slices )
    return(allTargets) 

#####
def start_letter_generator( universe , letter ) :
    for w in universe :
        if( w[0] == letter ) :
            yield w

#####
def start_letter_list( universe , letter ) :
    slgen = start_letter_generator( universe , letter ) 
    ret = [ ]
    for w in slgen :
        ret.append(w)
    return(ret) 

#####
def non_ASCII_generator( universe ) :
    isascii =  set( list("abcdefghijklmnopqrstuvwxyz") ) 
    for w in universe :
        if( w[0] in isascii ) :
            continue
        else :
            yield w


#####
def non_ASCII_list( universe ) :
    nonagen =  non_ASCII_generator( universe ) 
    ret = [ ]
    for w in nonagen :
        ret.append(w)
    return(ret) 


######
#  MAIN entry point
######
def orchestrator_function(context: df.DurableOrchestrationContext ):
    logging.info( "[debug] B0E5" ) 
    
    requestBody = context.get_input() 
    recreate = requestBody   # No conversion is required!
    
    # Arrives as a python dictionary already.
    logging.info( str(type(recreate)) )      
    logging.info( recreate )

    titleList = list(recreate.keys())
    #stopdbg = 2  # = len(titleList) 
    stopdbg = len(titleList) 

    if( stopdbg > 50 ) :
        logging.warning(ddos)
        return(ddos) 

    chatter("start timer on DataCleaners")
    tic = perf_counter_ns()
    parallelFanout = []

    # Run Datacleaner workers in parallel, one worker per book.
    for n in range(stopdbg) : 
        ctitle = titleList[n]
        request = { }
        request['longTitle'] = ctitle
        request['url'] = recreate[ctitle]
        request['maxBucket'] = 5000
        if ( ctitle in bookAuthors ) :
            request['author'] = bookAuthors[ctitle]
        else :
            request['author'] = "author"
    
        parallelFanout.append(  context.call_activity('Datacleaner', json.dumps(request)    )  )   
    
    # and wait for all Datacleaners to complete. 
    cleanStageResults = yield context.task_all(parallelFanout)
    toc = perf_counter_ns()
    elapse = (toc-tic) /  (10.0 ** 9)  
    logging.info( "[debug] data cleaning time {} sec".format(elapse) ) 

    # Flatten all books into a linear list of buckets.
    flattenedBs = [ ] 
    flat = 0
    for b in cleanStageResults :
        rebox = json.loads( "["  + b + "]"    )
        recDictoBuckets = rebox[0]
        for k in (recDictoBuckets.keys()) :
            flattenedBs.append(   recDictoBuckets[k]  )
            flat=1+flat

    flatBuckets = len(flattenedBs)
    chatter( "total buckets in all books {}".format( flatBuckets )  ) 

    if( flatBuckets > 800 ) :
        logging.warning(ddos)
        return(ddos)  

    chatter("start timer on Mappers")
    tic = perf_counter_ns()
    parallelFanout = []
    # Run Mappers in parallel, one Mapper per bucket.
    for n in range( flatBuckets ) : 
        bucket = flattenedBs[n]
        parallelFanout.append(  context.call_activity('Mapper', json.dumps(bucket)    )  ) 

    # and wait for all Mappers to complete. 
    mapStageResults = yield context.task_all(parallelFanout)

    toc = perf_counter_ns()
    elapse = (toc-tic) /  (10.0 ** 9)  
    logging.info( "[debug] Map time {} sec".format(elapse) )

    # Collect results of all Mappers
    corpusInvx = { } 
    n=0  
    for bkt in mapStageResults : 
        invindex = json.loads( bkt ) 
        corpusInvx[n] = invindex 
        n=1+n 


    '''
    At this point, corpusInvx  contains duplicated words appearing in many buckets.
    We need a word list with unique words which is sorted alphabetically.  
    '''

    # COMBINER stage.
    # combiner cannot be parallelized since we need unique words.
    chatter("COMBINER stage")
    wordSet = set([])
    for k in (corpusInvx.keys()) :
        curII = corpusInvx[k] 
        for word in (curII.keys()) :
            wordSet.add(word) 
    
    wordUniverse = sorted(wordSet) 

    lwu = len(wordUniverse)
    chatter("{} unique words in {} books".format( lwu , stopdbg )  )
    
    # Create 2 lists of words for load balancing among reducers.
    totalReducers = 2 
    allTargetClasses = balanced_slicer( wordUniverse , totalReducers ) 

    if ( verboseStats ) :
        tcs = len( list(  allTargetClasses.keys()  ))
        t=0
        for k in  (allTargetClasses.keys()) :
            reducerTargets = allTargetClasses[k]
            chatter( "size of reducerTargets {}".format( len(reducerTargets) ) )   
            t = t + len( reducerTargets ) 
        chatter( "total words {}.  in {} targetclasses".format(t,tcs) )  

    sharedBigScanner = json.dumps( corpusInvx ) 

    chatter("start timer on Reducers")

    # Spawn 2 reducer workers.   
    tic = perf_counter_ns()
    parallelFanout = []
    for k in  (allTargetClasses.keys()) :
        # Each reducer gets its own set of unique words. 
        reducerTargets = json.dumps( allTargetClasses[k] )
        payload = [ reducerTargets , sharedBigScanner ] 
        parallelFanout.append(  context.call_activity('Reducer', payload )  ) 

    # and wait for all Reducers to complete. 
    ReduceStageResults = yield context.task_all(parallelFanout)

    toc = perf_counter_ns()
    elapse = (toc-tic) /  (10.0 ** 9)  
    logging.info( "[debug] Reduce time {} sec".format(elapse) )

    InvertedIndex = { } 
    for reduced in ReduceStageResults :
        splitReduction = json.loads(reduced) 
        for k in (splitReduction.keys()) :
            InvertedIndex[k] = splitReduction[k]

    # Write results to Azure blob storage. 
    filesz = yield context.call_activity('Finalout', json.dumps(InvertedIndex) )

    orchCompleteReturn = "SUCCESS.  COMPLETED {} buckets over {} books. Inverted index {} bytes".format(flat, stopdbg , filesz )
    logging.info(orchCompleteReturn)
    return ( orchCompleteReturn ) 

main = df.Orchestrator.create(orchestrator_function)

