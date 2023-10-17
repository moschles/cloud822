/*
Coordinator

Communicates directly with replicas of a primary storage.
Acts as a gateway to networked storage for 
one client into any number of replicas.

Output format to each replica. serialized string. token delim = "|"
    GET,PUT,DEL
	key
	value
	clientUUID
	transactionID

Expected responses from a quorum of replicas. serialized string. token delim = "|"
    ACKG,ACKP,ACKD
	replicaName
	key
	value
	version
	tamperUUID
	logicalclock
	writetotal
	deletetotal
	clientUUID
	transactionID

attribution  :
    + https://stackoverflow.com/a/32175024
    + https://www.youtube.com/watch?v=Jo4ad1FR0Cs
 

last mod: Sunday, October 15, 2023 8:17:20 PM

*/

package com.core

// Java //
import java.io._
import java.net.{InetAddress,Socket,SocketException,SocketTimeoutException}
import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentLinkedQueue

// Scala // 
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.util.{Success, Failure}
import scala.math.abs

sealed case class Response(payload:String)
sealed case class Quorum(fifoSize:Int)

sealed class Replica ( var name:String , var IPaddr:String,  var port:Int )
 { } 

sealed case class ResponseBundle( ack:String, 
    key:Int, value:String,version:Int, tamperUUID:String,
    logicalclock:Int,writetotal:Int,deletetotal:Int,
    clientUUID:String,transactionID:Int ) 


// * COORDINATOR CLASS * //
class Coordinator( val ec: ExecutionContext, val globalTimeout:Int ) 
{
    private var replicas:List[Replica]= List[Replica]()
    private var status:Int = 1
    private val testQsz = 1
    private val deployQsz = 2
    private val quorumSize:Int = deployQsz
    private val debug = false
    
    // // 
    def decorate_replica(   name:String, IPaddr:String,  port:Int ):Unit = {
        replicas = (new Replica(name,IPaddr,port)) :: replicas  
    } 

    // // 
    def last_status:Int = status  

    // // 
    def client_command( cmd:String, clientUUID:String, transactionID:Int ):String = {
        assert( replicas.isEmpty == false ) 
        val storagesys = new StorageCommunication( ec , replicas, globalTimeout ) 
        val activepayload =  cmd + "|" + clientUUID + "|" + transactionID.toString
        val allResponses = storagesys.broadcast( activepayload , quorumSize )
        // status = storagesys.last_status 
        if( allResponses.isEmpty  ) {
            println("no response from any replica")
            ""
        }  else {
            val aRlist:List[String] = allResponses.toList
            if (debug) {   
                ("all responses :" :: aRlist).mkString("\n").toString
            } else {
                // de-serialize them
                val nonserial = for( r <- aRlist ) yield { deSerialize(r) }

                // only those that parsed
                val onlysome = nonserial.filter(_.isDefined)

                // de-box them
                val debox = for( elt<- onlysome) yield { elt.get }

                val oasz = debox.toList.length 

                if( oasz == 0 ) {
                    // We are done
                    ""
                } else {
                    if( oasz < quorumSize ) {
                        /* This code block handles scenarios in which a quorum
                           of replies did not arrive from primary storage.

                           We can still recover under the following conditions.
                           all of which must be true.

                           1. The client wants to GET

                           2. One or more of the replicas had a tamperUUID on
                            that <k,v> entry which matches the clientUUID that wants this data. 
                           */  
                        if( cmd.take(3) == "GET" ) {
                            // filter those entries that this client tampered with.
                            val readOnWritesq = debox.filter(_.tamperUUID == clientUUID)


                        } else {
                            // no dice
                            ""
                        }
                        
                    } else {
                    
                    }
                }
            }

        }
    } 

    // // 
    private def deSerialize( payload:String ):Option[ResponseBundle] = {
        try {
            val box = payload.split("\\|").toList.toVector
            val rb = new ResponseBundle(  box(0),
                box(1).toInt,box(2),box(3).toInt,box(4),box(5).toInt,
                box(6).toInt,box(7).toInt,box(8),box(9).toInt )
            Some(rb) 
        } catch {
             case e:Exception => None
        }
    }
} 


/* ///////
Communicate with the primary storage system. 
Network connects concurrently with all remote replicas. 
Handling outbound commands and inbound responses.
All replica responses are pooled into a threadsafe FIFO.
Process terminates early when a quorum of reponses are received. 
///// */
sealed class StorageCommunication ( val ec: ExecutionContext,  var replicas:List[Replica], val globalTimeout:Int )
{
 //  globalTimeout is wait time on each replica. 
  private val watcherTimeout: Double = 14.0 // Wait for quorum of replicas.
  private val FIFO = new ConcurrentLinkedQueue[String]( )
  private val promise:Promise[Int] = Promise[Int]( )
  private var status:Int = 1
  
  // Callback 
  promise.future.onComplete {
    case Success(p) =>  status = 1
    case Failure(ee) => status = (-1)
  }(ec)

    // non-blocking concurent thread
    //  assign one of these to each replica  
    // {{
  private def forward_to_replica(outbound:String, rep:Replica, timeoutsecs:Int ):Future[Response] = 
  Future {
    val channel = new ReplicaNetChannel( rep.name, rep.IPaddr, rep.port, timeoutsecs, 4 )
    var inbound:Option[String] = (channel.enabled()) match {
      case Some(i) => channel.forward_client_command( outbound )
      case _ => None
    }
    val rcontent = inbound match {
        case Some(r)=>r
        case _ => "ERRT"  // replica timeout error? 
    }
    FIFO.add(rcontent)
    Response(rcontent)
  }(ec)
  // }}  

    // Non-blocking thread. 
    // Remote replicas wil begin filling up a FIFO. This thread watches and
    // waits for the FIFO to be filled up to a quorum of responses. 
    //  When this is detected, a promise is fullfilled invoking a callback.  
  private def fifo_is_full( quorumSize:Int, waitT:Double ):Future[Quorum] = Future {
    val tic = System.nanoTime / 1.00e9d
    var promlock = "s"

    while((FIFO.size() < quorumSize) && (promlock=="s") ) {
      // busy wait
      val toc = System.nanoTime / 1.00e9d
      promlock = if( (toc-tic) < waitT ) "s" else "f"
    }

    promlock match {
      case "s" => promise.success(1)
      case _ => promise.failure( new Throwable("time out") )
    }

    // FIFO could grow larger from while conditional above, due to concurrency.
    Quorum( FIFO.size() )
  }(ec)

    // // //
  def last_status:Int = status 

    //  Broadcast a single client db command to all replicas. 
    // Returns the f-most earliest arriving responses from the replicas.
    //  For when there are (2f-1) total replicas. 
    def broadcast(payload:String, quorumSize:Int ) : IndexedSeq[String] = {
        val quorumStatuses = fifo_is_full(quorumSize, watcherTimeout )
        //Thread.sleep(5) // wind up the watcher  5ms 

        // Start thread on each channel for each replica.
        for( r <- replicas ) forward_to_replica(outbound=payload, rep=r, globalTimeout ) 

        val qr = Await.result(quorumStatuses, Duration.Inf )

        val firstResponders = {List(FIFO.size,quorumSize)}.min
        for( i <- 1 to firstResponders ) yield{ FIFO.poll() }
    }
}


/* //////
 Forward a client's command to a single replica, and block for a response from it.
  This will be executed asynchronously against other network traffic.

  Wait 'timeoutsecs' for a an open socket, attempting 'sockknock' times to connect
  throughout that time interval. 

  Forward the client command to the replica, then wait another 'timeoutsecs' seconds
  for the reply to return.    
/////// */
class ReplicaNetChannel(
        val replican:String = "00",
        val IPaddr:String="127.0.0.1", 
        val port:Int=8177,  
        val timeoutsecs:Int=20,
        val sockknock:Int=4) 
{
    private val timeout = (timeoutsecs)*1000 
    private var ia:Option[InetAddress] = None
    private var socket:Option[Socket] = None
    private var out:Option[ObjectOutputStream] =None
    private var in:Option[ObjectInputStream]= None
    
    // Refactor this code so replicas die gracefully.
    private var k=0
    while( (enabled().isEmpty) && k < sockknock ) {
        if (k>0) { Thread.sleep(timeout/sockknock) }
        ia =  try {Some(InetAddress.getByName(IPaddr)) } catch { case ex:Exception => None }
        socket =  try{Some(new Socket(ia.get,port))} catch { case ex:Exception => None }
        out = try {
            Some(new ObjectOutputStream(new DataOutputStream(socket.get.getOutputStream()))   )
        } catch {
            case ex:Exception => None
        }
        in = try {
            Some(new ObjectInputStream(new DataInputStream(socket.get.getInputStream()))   )  
        } catch {
            case ex:Exception => None
        }
        k=1+k
    }   

    if( enabled().isDefined ) {
        socket.get.setSoTimeout(timeout)
    } 
         
    def enabled(): Option[Int] = {
        val chk = out.isDefined && in.isDefined && ia.isDefined && socket.isDefined ;
        chk match {
            case true => Some(10)
            case false => None
        }
    }

    def forward_client_command( payload:String ) : Option[String] = {
        var response = None:Option[String]
        ( enabled() ) match {
            case Some(i) =>   
                try {
                    out.get.writeObject(payload)
                    
                    response = try {
                        Some( string_harbor(in) )
                    } catch {
                        case e: SocketTimeoutException  => {
                            //println( s"Replica timed out after $timeoutsecs sec" )
                            None
                        }
                        case e:Throwable => { 
                            //println("Throwable during readObject()")
                            //println(e.getStackTrace.mkString("\n"))
                            None 
                        }
                        case e:Exception => { 
                            //println("Exception during readObject()")
                            //println(e.getStackTrace.mkString("\n"))
                            None 
                            }
                    }
                    
                } catch {
                    case e: Exception => {  
                        //println(e.getStackTrace.mkString("\n"))
                        println( "replica%s refused outbound write".format(replican) ) 
                    } 
                } finally {
                    out.get.close()
                    in.get.close()
                    socket.get.close()
                }
            case None =>{ 
                println("cannot forward_client_command() until socket is healthy."); 
                println("Replica network channel should be recreated.")
            }
        }
        response
    }

    private def string_harbor( inloc:Option[ObjectInputStream] ):String = inloc.get.readObject().asInstanceOf[String] 

}