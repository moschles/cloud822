/*
Benchmark

Spawn several "client users" as concurrent threads.  
Clients send commands to a  coordinator, which dispatches those commands to a primary storage system.
The coordinator chooses among the responses, sending a winner to the client as a single response.  

Each client measures the RTT of upstream/downstream cycle. 
Each client measures the throughput in  messages/second. 

No portion of this benchmark is blocking. 
Clients only block themselves as a means of measuring RTT. 
The coordinator receives commands from clients through independent pipes.
The coordinator spawns an independent thread for each replica.  

To obtain  non-blocking for multiple clients by pairing a coordinator
thread against each client thread. 

attribution: 
https://stackoverflow.com/a/64086683


last mod: 
Wednesday, October 11, 2023 8:40:35 PM
*/

package com.core

// Java //
import java.util.concurrent.ConcurrentLinkedQueue
import java.io.{File,BufferedWriter,FileWriter}
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

// Scala // 
import scala.concurrent.ExecutionContext
import scala.util.Random
import collection.mutable.Queue


sealed case class Cargo(cmd:String, uuid:String, transID:Int) 


// ** PIPE CLASS ** //

sealed class Pipe ( )
{
    private val outbox = new ConcurrentLinkedQueue[Cargo]()
    private val inbox = new ConcurrentLinkedQueue[String]() 

    def all_out():List[Cargo] = {
        var ao:List[Cargo] = List()
        while( !outbox.isEmpty ) {
            ao = (outbox.poll()) :: ao 
        }
        ao
    }
    def listen():Option[String] = if( inbox.isEmpty ) { None } else { Some(inbox.poll()) }  
    def respond( netrecv:String ) = inbox.add(netrecv)
    def command_upload( cmdu:String, uuidu:String, transIDu:Int ) = {
        val ccc:Cargo = Cargo(cmdu,uuidu,transIDu)
        outbox.add( ccc  )
    } 
}


// ** BENCHMARK CLASS ** //

class Benchmark( 
    clients:List[(String,Int)], 
    replicas:List[(String,String,Int)] ,
    recess:Int,
    timeout:Int, 
    ec: ExecutionContext   ) 
{
    private val total = clients.length
    private val span = (1 to total).toList 
    
    println("Benchmark creation ======== ")
    // Need pipes
    println(" pipes")
    val pipes:Vector[Pipe] =  {
        for(n<-span)  yield { new Pipe() }
    }.toVector 

    // Need round trip times
    println(" round trip times" ) 
    val rttResults:Vector[Queue[String]] = {
        for(n<-span) yield { new Queue[String]() }
    }.toVector 

    // Need throughput marks
    println(" throughput marks")
    val thruResults:Vector[Queue[Double]] = {
        for(n<-span) yield { new Queue[Double]() }
    }.toVector

    // Need decorated coordinators 
    println(" coordinators, decorate")
    val cooentourage:Vector[Coordinator] = {
        for(n<-span) yield { 
            val coor = new Coordinator(ec,timeout)
            for(r<-replicas) {
                coor.decorate_replica(r._1,r._2,r._3) 
            }
            coor 
        }
    }.toVector

    // Need coordinator threads
    println(" coordinator workers")
    assert( cooentourage.size == total )
    assert( pipes.size == total ) 
    val cooworkers:Vector[CoordinatorWorker] = {
        for( i <-  (0 until total) ) yield {
            new CoordinatorWorker( cooentourage(i) , pipes(i) ) 
        }
    }.toVector

    // Need client threads
    println(" client workers")
    assert( rttResults.size == total )
    assert( thruResults.size == total ) 
    val clientworkers:Vector[ClientWorker] = {
        for( i <-  (0 until total) ) yield {
            new ClientWorker( clients(i)._1 , clients(i)._2, 
                pipes(i),
                rttResults(i),
                thruResults(i),
                recess,  128127*(i+1)  ) 
        }
    }.toVector


    //  <------- END CONSTRUCTOR -------->  //

    // // / / 

    def time_trials () = { 
        // Start coordinator threads
        println("start coordinators")
        (0 until total).foreach( w => cooworkers(w).start() )

        println("warm up coordinator threads" )
        Thread.sleep(700) 

        // Start client threads
        println("start clients")
        for(w <- (0 until total)  ) {
            Thread.sleep(1783)
            clientworkers(w).start() 
        }

        // Block for client terminations.
        clientworkers.foreach( z => z.join() ) 

        // Block for coordinator terminations.
        cooworkers.foreach( z => z.join() )
    }

    // // //
    def store_results() = {

        // Round trip times for each client.
        for(i <- (0 until total) ) {
            val filen = "results_%s_%d".format( clients(i)._1 , clients(i)._2 )
            store_rtt_file(  rttResults(i) , filen ) 
        }

        // Throughput for each client
        val thrupall:Vector[String] = { 
            for(i <- (0 until total) ) yield {
                val tic:Double = thruResults(i).dequeue 
                val toc:Double = thruResults(i).dequeue 
                val thruF:Double = ( clients(i)._2.toDouble ) / (toc-tic)
                val trunt:Double = (toc-tic)
                println( "client %s total runtime ".format(clients(i)._1) + trunt.toString ) 
                (f"$thruF%.7f")
            }
        }.toVector
            
        for(i <- (0 until total) ) {
            val filen = "throughput_%s_%d".format( clients(i)._1 , clients(i)._2 )
            store(  thrupall(i) , filen ) 
        }
        
    }


    // // //
    def store_rtt_file( Q:Queue[String], rttoname:String ) {
        try { 
            val file = new File(rttoname )
            val bw = new BufferedWriter(new FileWriter(file,Charset.forName("UTF8")))
            bw.write( Q.mkString("\n") )
            bw.close()
        }    catch {
            case _ : Exception => println("output file is no longer up-to-date.")
        } finally {
            Q.clear()
        }
    }  

    // // //
    def store( fcont:String , fname:String ) {
        try { 
            val file = new File( fname )
            val bw = new BufferedWriter(new FileWriter(file,Charset.forName("UTF8")))
            bw.write( fcont )
            bw.close()
        }    catch {
            case _ : Exception => println("output file is no longer up-to-date.")
        }
    }

}

// ** ** ** //
// CLIENT THREAD //////////////////// //
sealed case class ClientWorker( UUID:String, count:Int, 
    pipe:Pipe, Qrtt:Queue[String], Qthruputm:Queue[Double], 
    recess:Int, tidOffset:Int ) extends Thread("ClientWorker") 
{  
    val rng = new scala.util.Random
    private val Alphanumeric = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes
    private val dbCommand = Map(0->"PUT",  1-> "GET", 2->"DEL" )
    val seed = Integer.parseInt( UUID.replace("-","0").take(7) , 16 )
    rng.setSeed(  seed.toInt )
  
    // Generate fast random payloads.  We are benchmarking network round trip time, not 
    //   string generation time. 
    def mkStr(chars: Array[Byte], length: Int): String = {
        val bytes = new Array[Byte](length)
        for (i <- 0 until length) bytes(i) = chars(rng.nextInt(chars.length))
        new String(bytes, StandardCharsets.US_ASCII)
    }
    def nextAlphanumeric(length: Int): String = mkStr(Alphanumeric, length)

    def random_kv( vlength: Int ):String = {
        val cmd = dbCommand( rng.nextInt(3) )
        val key = 1 + rng.nextInt(9);
        val value =nextAlphanumeric(vlength) ;
        cmd + "|" + key.toString + "|" + value 
    }

    override def run(): Unit = {
        Qthruputm += System.nanoTime / 1.00e9d
        for( c <- (1 to count) ) {
            if( c > 1 && recess>0) { Thread.sleep(recess) } 
            pipe.command_upload(  (random_kv(71)) , UUID , c + tidOffset ) 
            val tic = System.nanoTime / 1.00e9d

            // Client will block on a busy wait.
            var hear:Option[String] = pipe.listen()
            while( hear == None ) {
                hear = pipe.listen()
            }


            val toc = System.nanoTime / 1.00e9d
            val rtt = toc-tic 
            Qrtt += { if (true)  (s"$c " + f"$rtt%.7f") else "NaN" }
            println(hear.get) 
        }
        Qthruputm += System.nanoTime / 1.00e9d
        pipe.command_upload("SIG_KILL", UUID, (-1) )
    }
}


// ** ** ** //
// COORDINATOR THREAD //////////////////////////  //
sealed case class CoordinatorWorker( coo:Coordinator,  pipe:Pipe ) extends Thread("CoordinatorWorker") 
{
    override def run(): Unit = { 
        var sigkill:Boolean = false

        while( ! sigkill ) {
            val msgs:List[Cargo] = pipe.all_out()
            for(m<-msgs) m.cmd match {
                case "SIG_KILL" => sigkill=true
                case _ => { 
                    val respSingleton:String = coo.client_command( m.cmd , m.uuid, m.transID )
                    pipe.respond(respSingleton)
                    //pipe.respond(m.cmd)  
                }
            }
        }
    }
}
