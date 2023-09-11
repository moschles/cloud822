/*
Ping client  

This ping client now boasts recovery from timeouts from the server. 

last mod: Saturday, September 9, 2023 6:14:31 PM
*/

package com.core

import java.io._
import java.net.{InetAddress,Socket,SocketException,SocketTimeoutException}

class Pingc(val IPaddr:String="127.0.0.1", val port:Int=8177, val count:Int=1 , val timeoutsecs:Int=30 ) {
    private val payload = "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F"
    private val timeout = (timeoutsecs)*1000 
    private var out = None:Option[ObjectOutputStream]
    private var in = None:Option[ObjectInputStream]
    private var ia = None: Option[InetAddress]
    private var socket = None: Option[Socket]  

    //  specified.  CS722 Cloud  project 0.
    assert( payload.length() == 128 )   
    //  specified.  CS722 Cloud  project 0.
    assert(  payload.getBytes().length == 128 )  

    print ("begin socket creation. . .")
    
    private var w = None:Option[Int]
    while(w.isEmpty) {
        ia =try { Some(InetAddress.getByName(IPaddr)) } catch { case e: Exception => None}
        socket = try{ Some(new Socket(ia.get,port)) }   catch { case e: Exception => None}
        w = (ia,socket) match {
              case (None,_)| (_, None) =>{
                println("Waiting for server . . .")
                Thread.sleep(5000)
                println("retrying,") 
                None
              } 
              case (Some(x),Some(y)) => {
                print ("success.")
                Some(10)
              } 
        }
    } 


    try {
        assert( socket.isDefined )
        socket.get.setSoTimeout(timeout)
        out = Some(new ObjectOutputStream(new DataOutputStream(socket.get.getOutputStream()))   )
        in = Some(new ObjectInputStream(new DataInputStream(socket.get.getInputStream()))   )   
    } catch {
        case ex:Exception => { 
            println("Failed to create io streams."); 
            //println(ex.getStackTrace.mkString("\n")) 
        }
    }

    def enabled(): Option[Int] = {
        val chk = out.isDefined && in.isDefined && ia.isDefined && socket.isDefined ;
        val ena =  chk match {
            case true => Some(4)
            case false => None
        }
        ena
    } 

    def send_ping( ) : Option[Double] = {
        var success = None:Option[Double]
        var response = None:Option[String]
        ( enabled() ) match {
            case Some(i) =>   
                try {
                    out.get.writeObject(payload)
                    var tic:Double = System.nanoTime / 1.00e9d  
                    out.get.flush()
                    
                    response = try {
                        Some( string_harbor(in) )
                    } catch {
                        case e: SocketTimeoutException  => {
                            println( s"Server timed out after $timeoutsecs sec" )
                            Some("tout")
                        }
                        case e:Throwable => { 
                            println("Throwable during readObject()")
                            //println(e.getStackTrace.mkString("\n"))
                            None 
                        }
                        case e:Exception => { 
                            println("Exception during readObject()")
                            //println(e.getStackTrace.mkString("\n"))
                            None 
                            }
                    }
                    var toc:Double = System.nanoTime / 1.00e9d
                    var elapse = 1000.0 * (toc-tic)
                    var status:String = response match {
                        case Some(x) =>
                            if( payload.length == x.length ) {
                                success = Some(elapse)
                                ""
                            } else {
                                if( x=="tout" ) {
                                    success= Some(-1.0); "timeout"
                                } else { "Response was mangled." }
                            }        
                        case None =>"Response was mangled" 
                    }
                    print(s"$status [$IPaddr] " + f"   $elapse%1.1f ms" )
                } catch {
                    case e: Exception => {  
                        println(e.getStackTrace.mkString("\n")) 
                    } 
                    success = None  
                } finally {
                    out.get.close()
                    in.get.close()
                    socket.get.close()
                }
            case None =>{ 
                println("cannot send_ping() until socket is healthy."); 
                println("Pingc object should be recreated.")
                success = None
            }
        }
        success
    }

    private def string_harbor( inloc:Option[ObjectInputStream] ):String = inloc.get.readObject().asInstanceOf[String] 

}