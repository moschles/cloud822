/*  ****
Pong server

This server responds to incoming pings in a non-blocking manner.
This is acheived by creating a new thread for each listener.  

date: Thursday, September 7, 2023 11:57:21 PM
**** */

package com.core

import java.io._
import java.net.{ServerSocket,Socket,SocketException}

class Pongs(  val port:Int=8177) {
    private var listener = None: Option[ServerSocket]
    
    println("begin socket creation. . .")
    
    while(listener.isEmpty) {
        listener = try{ Some(new ServerSocket(port)) }   catch { case e: Exception => None}
        listener match {
              case None =>{
                println("Fail. Waiting to recreate . . .")
                Thread.sleep(5000)
                println(s"recreating on port $port") 
              } 
              case Some(x) => println(s"success.")
        }
    } 

    def enabled(): Option[Int] = 
        listener match {
            case Some(x) => Some(10)
            case None => None
        }
    

    def wait_ping( ) : Option[Int] = {
        val health:Option[Int] = enabled()
        val success:Option[Int]= health match {
                case Some(i) =>
                    println(s"listening on port $port")
                    while(true) {
                        // block for a ping
                        var puppet:Socket = listener.get.accept() 
                        // process pong with non-blocking thread spawn.
                        new ListenWorker(puppet).start()
                    }   
                    Some(10)  
                case None =>{ 
                    println("cannot wait_ping() until listener is healthy."); 
                    println("Pongs class should be recreated.")
                    None
                }
        }
        success
    }

}


// Thread for listener //
case class ListenWorker(socket: Socket) extends Thread("ListenWorker") {

  override def run(): Unit = { 
    var outsn = None:Option[ObjectOutputStream]
    outsn = try {
            Some(new ObjectOutputStream(new DataOutputStream(socket.getOutputStream()))) 
        } catch { 
            case e: Exception => None 
        }
    var insn = None:Option[ObjectInputStream] 
    insn = try{ 
            Some(new ObjectInputStream(new DataInputStream(socket.getInputStream()))) 
        } catch {
            case e: Exception => None
        }
    (outsn,insn) match {
        case (None,_)|(_,None) => println("Could not create io streams.")  
        case (Some(out),Some(in)) => { 
             
            try { 
                val payload:String = in.readObject().asInstanceOf[String];
                out.writeObject(payload)
                out.flush()
                val sz = payload.getBytes().length
                val clientIPv4 = socket.getRemoteSocketAddress().toString()
                println(s"returned $sz bytes to $clientIPv4")
                in.close()
                out.close()
                socket.close()
                ()
            } catch {
                case e: SocketException =>
                    () // avoid stack trace when stopping a client with Ctrl-C
                case e: IOException =>
                    e.printStackTrace()
                    ()
            }
         }
    }

  }

}
