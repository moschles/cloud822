'''
+ ACTIVITY +

    Datacleaner reads a single book from remote URL.

    1 Downloads a single book from given URL.
    2 Removes all punctuation.
    3 Filters common words.
    4 Returns a dictionary of buckets of words in lowercase. 

    date:
    Tuesday, November 28, 2023 6:51:54 PM

'''

import logging
import json 
import urllib3
import re
from time import perf_counter_ns

'''
########################  force azure to change the code 7-5 $
'''


def bad_words() :
    articles="the,an,and,or,not,as,no,nt,ve,t,x,a,b,c,d,e,o,k,s,al,de,del,el,la,et,ein,eine,von,http,net,com,edu"
    prepos="so,to,of,in,into,for,on,with,at,by,from,out,up,if,than,just,which,after,before,over,back,there,daily,yearly"
    trans="very,too,often,thus,though,further,furthermore,finally,etc,because,but,however,therefore,ultimately"
    possess="his,her,my,their,your,its,our,whose,theirs"
    folks="i,I,it,he,you,they,we,she,me,him,them,these,this,those,that,us,mr,mrs,ms,miss,st,saint"
    interg="what,where,who,how,why,when"
    wtime="do,did,does,now,then,will,can,could,would,should,must,shall,had,has,have,having,haven,hasn,won,until,til"
    verb="be,go,get,see,look,come,want,say,time,make,was,been,were,went,got,saw,looked,came,wanted,said,timed,made,"+\
    "is,are,goes,gets,sees,looks,comes,wants,says,times,makes,being,going,getting,seeing,looking,coming,wanting,saying,timing,making"
    mind="thought,think,thinks,thinking,knew,know,knows,knowing,gave,give,gives,giving,supposed,suppose,supposses,supposing,worked,work,works,working,told,tell,tells,telling"
    obj="chapter,people,person,persons,day,days,year,years,none,first,one,two,three,only,some,more,any,much,many,all,every"
    filr="well,uh,uhm,um,umm,oh,also,ok,okay,good,nah,nope,like,kay,yw,lol,thx,hi,bye,hey,huh"   
    roman="ii,iii,iv,v,vi,vii,viii,xi,xii,xiii,xiv,xv"
    rr = articles.split(',') + prepos.split(',') + trans.split(',') + possess.split(',') +\
    folks.split(',') + interg.split(',') + wtime.split(',') + verb.split(',') +\
    mind.split(',') + obj.split(',') + filr.split(',') + roman.split(',')    
    return ( set(rr) ) 


'''
########################
'''
def is_non_empty( ss ) :
    if ss : 
        return True
    else :
        return False


'''
########################
'''
def filter_common( origstrl, wexclude ) :
    for wd in origstrl :
        if( is_non_empty(wd) ) :
            if( wd.isalnum() ): 
                if( not (wd in wexclude) ) :
                    yield wd

'''
Function returns a python generator of strings
'''
def book_word_generator( rawbook , wexclude ) :
    # Regex cannot match against these escape characters.
    doc = rawbook.replace('\n', ' ')
    doc = doc.replace('\t', ' ')
    doc = doc.replace('\r', ' ')
    doc = doc.replace('&', ' ')
    backslash = '''\\''' 
    frontslash = '''/'''
    doc = doc.replace(  backslash , ' ')
    doc = doc.replace(  frontslash , ' ')
    
    # Make lowercase.
    doc = doc.lower()

    # Remove punctuation and split along punctuation in one call. 
    regex = r'\s+|[][1?`234567890~!@#$%^*()_=+":<>{},;.-]\s*'
    rawwordl = re.split(regex, doc)  # zang

    return(  filter_common(rawwordl, wexclude ) ) 


'''
########################
'''
def get_next_bucket( wGenerator , bucketSize ) :
    bucket=[]
    morewords=True
    try :
        for i in range(bucketSize) :
            bucket.append( next(wGenerator) )
    except StopIteration:
        morewords=False
        pass
    return( (bucket,morewords) )



'''
########################
'''
def scoped_http_try( targeturl ) -> str :
    try :
        httppool = urllib3.PoolManager()
        response = httppool.request('GET', targeturl ) 
        return( response.data.decode('utf-8') )     
    except:
        return("remote book connection error")


'''
Datacleaner with input payload trigger 
'''
def main(payload: str) -> str:
    # De-serialize input arguments from payload
    rebox = json.loads( "["  +  payload + "]" )
    recreate = rebox[0]
    author = recreate['author']
    targeturl = recreate['url']
    maxBucket = recreate['maxBucket'] 
    longTitle = recreate['longTitle'] 
 
    tic = perf_counter_ns()
    bookdata = scoped_http_try( targeturl )
    toc = perf_counter_ns()

    elapse = (toc-tic) /  (10.0 ** 9)  
    logging.info( "[debug] download time {} sec".format(elapse) ) 

    tic = perf_counter_ns()  # ///////////////////////////////

    lenbook = len(bookdata)
    logging.info( "total characters {}".format(lenbook) )   
    leadingmetadata  = bookdata.find("START OF THE PROJECT GUTENBERG EBOOK")
    trailingmetadata = bookdata.find("END OF THE PROJECT GUTENBERG EBOOK")
    logging.info("end of leading    metadata {}".format(leadingmetadata ) )
    logging.info("start of trailing metadata {}".format(trailingmetadata ) )
    bslice = 0 if leadingmetadata==(-1) else (leadingmetadata+53)
    eslice = (lenbook) if trailingmetadata==(-1) else (trailingmetadata-6)   
    bookdata = bookdata[bslice:eslice]
    lenbook = len(bookdata)
    logging.info( "cropped metadata {}".format(lenbook) )

    badwords = bad_words()
    bgen = book_word_generator( bookdata , badwords )

    # Break up into buckets of words. 
    buckets = { }
    bno = 1
    genhasmore = True
    while( genhasmore ) :
        bucketNth,genhasmore = get_next_bucket( bgen , maxBucket )
        bucketNth.append( longTitle ) # Tag this bucket with book title
        bname = "buck" + str(bno) 
        bucketNth.append( bname ) # Tag this bucket with bucket number
        buckets[bname] = bucketNth
        bno=1+bno  

    
    toc = perf_counter_ns() # ///////////////////////////////
    elapse = (toc-tic) /  (10.0 ** 9)  
    logging.info( "[debug] filtering time {} sec".format(elapse) )

    logging.info("Stats for book " + longTitle + ".  (by " + author + ")" ) 
    bucketCount = 0
    totalWords = 0
    for k in (buckets.keys()) :
        bkt= buckets[k]
        logging.info("  {}  {} words".format(k, len(bkt) ) ) 
        totalWords = len(bkt) + totalWords
        bucketCount = 1 + bucketCount 

    logging.info("totals:  {} words in {} buckets".format(totalWords,bucketCount ) )
    return( json.dumps(buckets) )
