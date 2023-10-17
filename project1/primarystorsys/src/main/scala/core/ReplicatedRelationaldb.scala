/*  ****
A relational data store is based on a scala Map collection.
A concurrent Map is threadsafe. It allows
multiple clients to tamper with this data store asynchronously.

TODO :  Fix the race condition on logical clocks.  
1.) 
One way to fix this is to have each atomic PUT,GET,DEL return
the logical clock that resulted from the command itself. This cannot
be solved by locking separately on a clock() method. 
2.)
Another way to fix this is to store clock counts on every entry
of the database. 

TODO: (?)  Never actually delete entries, but mark them for deletion.
If a client DELETE()'s  that entry, the replica will
always expose that deletion to it immediately, even if all the
other replicas are lagged behind.  This is interesting in that
the version number will increase on deleted entries. 

date: 
Monday, October 9, 2023 7:39:05 PM
**** */

package com.core

import scala.collection.mutable.{Map,SynchronizedMap, HashMap}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore

/* *** */
class dbValue( 
    val content:String , 
    val version:Int , 
    val tamperUUID:String ) 
{
	override def toString: String = s"($content, $version, $tamperUUID)"
}

// /// // 
class ReplicatedRelationaldb( ) {
    private val bucket = new ConcurrentHashMap[Int, dbValue]()
    private var logicalclock = 0
    private var writetotal = 0 
    private var deletetotal = 0 
    private var semaphore   = new Semaphore(16,true)

    //

    // //  PUBLIC INTERFACE // // 
    override def toString: String = bucket.toString() 

    def GET( key:Int ):Option[(String,Int,String)] = critical_command("G",key,None,None) 
  

    def PUT( key:Int, value:String, clientUUID:String ):Int = 
        critical_command("P",key,Some(value),Some(clientUUID) ).get._2
    


    def DELETE( key:Int ):Unit ={
        critical_command("D",key,None,None) 
    }

    def clocks : (Int,Int,Int) = (logicalclock,writetotal,deletetotal) 
    
    // //  PRIVATE INTERNALS // // 

    private def critical_command( 
        cmd:String, 
        key:Int, 
        value:Option[String], 
        clientUUID:Option[String] ) : Option[(String,Int,String)] = 
    {
        semaphore.acquire
        tick()
        val ret = cmd match {
            case "G" => {
                if (bucket.containsKey(key))  {
                    val bk = bucket.get(key) 
                    Some(  (bk.content,bk.version,bk.tamperUUID)   )
                }  else {
                    None
                }        
            }
            case "P" => {
                writetotal = 1+ writetotal
                if(bucket.containsKey(key) ) {
                    val oldent = bucket.get(key)
                    val newent = new dbValue( value.get, 1+oldent.version, clientUUID.get  )
                    bucket.remove(key) 
                    bucket.put(key , newent)
                    Some(   (value.get,newent.version,clientUUID.get)  ) 
                } else {
                    val newent = new dbValue( value.get, 0, clientUUID.get ) 
                    bucket.put(key , newent)
                    Some(   (value.get,0,clientUUID.get)  )
                }
            }
            case _ => {
                deletetotal =1+ deletetotal
                bucket.remove(key)
                None        
            }
        }
        semaphore.release
        ret
    }
    
    private def tick():Unit = {
        logicalclock = 1+ logicalclock
    }
}
