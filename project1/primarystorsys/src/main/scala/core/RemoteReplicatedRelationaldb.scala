/*  ****
The replicated relational data store is made to be remote by 
this child class that extends with networking.
Thus we have a Remote, Replicated Relational database.  

Expected input. serialized string. token delim = "|"
    GET,PUT,DEL
	key
	value
	clientUUID
    transactionID 

Produces output response. serialized string. token delim = "|"
    ACKG,ACKP,ACKD,ERRC,ERRP,ERRD,ERRG 
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

date: 
Thursday, October 5, 2023 11:21:55 PM
**** */

package com.core
import java.io._
import java.net.{ServerSocket,Socket,SocketException}


class RemoteReplicatedRelationaldb( name:String, port:Int, network:Boolean, silent:Boolean ) extends ReplicatedRelationaldb 
{
    private var listener = None: Option[ServerSocket]
    
    if( network ) { 
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
    } 

    def get_name:String = {name} 
    def get_port:Int = {port}
    def quiet:Boolean = {silent} 

    def enabled(): Option[Int] = 
        listener match {
            case Some(x) => Some(10)
            case None => None
        }
    

    def wait_server( ) : Option[Int] = {
        val health:Option[Int] = enabled()
        val success = health match {
                case Some(i) =>
                    println(s"listening on port $port")
                    while(true) {
                        // block for a command from coordinator
                        var puppet:Socket = listener.get.accept() 
                        // process command with non-blocking thread spawn.
                        new ListenWorker(puppet, this ).start()
                    }   
                    Some(10)  
                case None =>{ 
                    println("cannot wait_server() until listener is healthy."); 
                    println("RemoteReplicatedRelationaldb class should be recreated.")
                    None
                }
        }
        success
    }     

    def parser_test( payload: String ):String = {
        println("raw payload {", payload )  
        val rrdb = this
                val tt:List[Option[Any]] = Deserializer.deserialize(payload)
                val nones = tt.count(_==None)

                val outgoing:String =  nones match {
                    case 0 => {
                        val clcmd:String = tt.head.get.asInstanceOf[String]
                        val key:Int = tt(1).get.asInstanceOf[Int]
                        val clientUUID:String =  tt(3).get.asInstanceOf[String]
                        val transactionID:Int = tt(4).get.asInstanceOf[Int]
                       
                        clcmd match {
                            case "GET" => {
                                //  GET()  returns  (content,version,tampererID) 
                                val gg:Option[(String,Int,String)] = rrdb.GET( key )

                                //  (logical clock, writetotal, deletetotal)
                                val dbclocks = rrdb.clocks
                                val ackn:String =  gg match { 
                                    case Some(g) => "ACKG" 
                                    case None => "ERRG"   
                                }
                                val value:String =  gg match { 
                                    case Some(g) => g._1 
                                    case None => ""   
                                }
                                val version:Int = gg match { 
                                    case Some(g) => g._2 
                                    case None => (-1)   
                                }
                                val tamper:String = gg match { 
                                    case Some(g) => g._3 
                                    case None => "E440-E440"  
                                } 
                                val logicalclock:Int = dbclocks._1
                                val writetotal:Int = dbclocks._2
                                val deletetotal:Int = dbclocks._3

                                Deserializer.serialize( ackn, 
                                    get_name,
                                    key, value,  version, tamper,
                                    logicalclock, writetotal, deletetotal, 
                                    clientUUID, transactionID )  

                            }
                            case "PUT" => {
                                val value:String = tt(2).get.asInstanceOf[String]
                                val tamper:String = clientUUID
                                val ackn:String = "ACKP"
                                val version:Int = rrdb.PUT( key, value, clientUUID )
                                val dbclocks = rrdb.clocks
                                val logicalclock:Int = dbclocks._1
                                val writetotal:Int = dbclocks._2
                                val deletetotal:Int = dbclocks._3

                                Deserializer.serialize( ackn, 
                                    get_name,
                                    key, value,  version, tamper,
                                    logicalclock, writetotal, deletetotal, 
                                    clientUUID, transactionID )

                            } 
                            case _ => { // This case will be DELETE
                                val value:String = tt(2).get.asInstanceOf[String]
                                val tamper:String = tt(3).get.asInstanceOf[String]
                                val ackn:String = "ACKD"
                                val version:Int = (-1) 
                                rrdb.DELETE( key )
                                val dbclocks = rrdb.clocks
                                val logicalclock:Int = dbclocks._1
                                val writetotal:Int = dbclocks._2
                                val deletetotal:Int = dbclocks._3

                                Deserializer.serialize( ackn, 
                                    rrdb.get_name,
                                    key, value,  version, tamper,
                                    logicalclock, writetotal, deletetotal, 
                                    clientUUID, transactionID )
                            } 
                        }
                    }
                    case _ => ErrorMachine.general_error_outbound(get_name , get_port) 
                }
                outgoing
    }
}


// Thread for listener //
sealed case class ListenWorker(socket: Socket, rrdb:RemoteReplicatedRelationaldb ) extends Thread("ListenWorker") {

  override def run(): Unit = { 
    var outsn :Option[ObjectOutputStream] = try {
            Some(new ObjectOutputStream(new DataOutputStream(socket.getOutputStream()))) 
        } catch { 
            case e: Exception => None 
        }
    var insn :Option[ObjectInputStream] = try{ 
            Some(new ObjectInputStream(new DataInputStream(socket.getInputStream()))) 
        } catch {
            case e: Exception => None
        }
    (outsn,insn) match {
        case (None,_)|(_,None) => println("Could not create io streams.")  
        case (Some(out),Some(in)) => { 
             
            try { 
                val payload:String = in.readObject().asInstanceOf[String];
                  
                val tt:List[Option[Any]] = Deserializer.deserialize(payload)
                val nones = tt.count(_==None)

                val outgoing:String =  nones match {
                    case 0 => {
                        val clcmd:String = tt.head.get.asInstanceOf[String]
                        val key:Int = tt(1).get.asInstanceOf[Int]
                        val clientUUID:String =  tt(3).get.asInstanceOf[String]
                        val transactionID:Int = tt(4).get.asInstanceOf[Int]
                       
                        clcmd match {
                            case "GET" => {
                                //  GET()  returns  (content,version,tampererID) 
                                val gg:Option[(String,Int,String)] = rrdb.GET( key )

                                //  (logical clock, writetotal, deletetotal)
                                val dbclocks = rrdb.clocks
                                val ackn:String =  gg match { 
                                    case Some(g) => "ACKG" 
                                    case None => "ERRG"   
                                }
                                val value:String =  gg match { 
                                    case Some(g) => g._1 
                                    case None => ""   
                                }
                                val version:Int = gg match { 
                                    case Some(g) => g._2 
                                    case None => (-1)   
                                }
                                val tamper:String = gg match { 
                                    case Some(g) => g._3 
                                    case None => "E440-E440"  
                                } 
                                val logicalclock:Int = dbclocks._1
                                val writetotal:Int = dbclocks._2
                                val deletetotal:Int = dbclocks._3

                                Deserializer.serialize( ackn, 
                                    rrdb.get_name,
                                    key, value,  version, tamper,
                                    logicalclock, writetotal, deletetotal, 
                                    clientUUID, transactionID )  

                            }
                            case "PUT" => {
                                val value:String = tt(2).get.asInstanceOf[String]
                                val tamper:String = clientUUID
                                val ackn:String = "ACKP"
                                val version:Int = rrdb.PUT( key, value, clientUUID )
                                val dbclocks = rrdb.clocks
                                val logicalclock:Int = dbclocks._1
                                val writetotal:Int = dbclocks._2
                                val deletetotal:Int = dbclocks._3

                                Deserializer.serialize( ackn, 
                                    rrdb.get_name,
                                    key, ".",  version, tamper,
                                    logicalclock, writetotal, deletetotal, 
                                    clientUUID, transactionID )

                            } 
                            case _ => { // This case will be DELETE
                                val value:String = tt(2).get.asInstanceOf[String]
                                val tamper:String = tt(3).get.asInstanceOf[String]
                                val ackn:String = "ACKD"
                                val version:Int = (-1) 
                                rrdb.DELETE( key )
                                val dbclocks = rrdb.clocks
                                val logicalclock:Int = dbclocks._1
                                val writetotal:Int = dbclocks._2
                                val deletetotal:Int = dbclocks._3

                                Deserializer.serialize( ackn, 
                                    rrdb.get_name,
                                    key, ".",  version, tamper,
                                    logicalclock, writetotal, deletetotal, 
                                    clientUUID, transactionID )
                            } 
                        }
                    }
                    case _ => ErrorMachine.general_error_outbound(rrdb.get_name , rrdb.get_port) 
                }
                val rname = rrdb.get_name    
                 
                out.writeObject(outgoing)
                out.flush()
                val sz = outgoing.getBytes().length
                val clientIPv4 = socket.getRemoteSocketAddress().toString()
                if( ! rrdb.quiet ) {
                    println(s"replica$rname recv $payload" )
                    println(s"returned $sz bytes to $clientIPv4")
                }
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

private object Deserializer  
{
    def deserialize( payload : String ):List[Option[Any]] ={  
        val tokens:Vector[String] = payload.split("\\|").toList.toVector
        if (tokens.size == 5) {
             val cmd = tokens(0) match {
                case "GET" => Some("GET")
                case "PUT" => Some("PUT")
                case "DEL" => Some("DEL")
                case _ => None
             }
             val key = try { Some(tokens(1).toInt) } catch { case e:Throwable => None }
             val value = Some(tokens(2))
             val cid = Some(tokens(3))
             val trans = try { Some(tokens(4).toInt) } catch { case e:Throwable => None } 
            List(cmd,key,value,cid,trans) 
        }else {
            List(None,None,None,None,None)
        }
    }

    def serialize( ackn:String, 
        name:String,
        key:Int, value:String,  version:Int, tamper:String,
        logicalclock:Int, writetotal:Int, deletetotal:Int, 
        clientUUID:String, transactionID:Int ) : String = {
            ackn + "|" +
            name + "|" +
            key.toString + "|" +
            value + "|" +
            version.toString + "|" +
            tamper + "|" +
            logicalclock.toString + "|" +
            writetotal.toString + "|" +
            deletetotal.toString + "|" +
            clientUUID + "|" + transactionID.toString 
        }      
}

/*
ACKG,ACKP,ACKD,ERRC,ERRP,ERRD,ERRG 
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
*/
private object ErrorMachine {

    def general_error_outbound(name:String, port:Int ):String = {
        val pts = port.toString 
        "ERRC|"+ name + "|-1|v|-1|0|-1|-1|-1|" + pts + "|" + pts
    }

} 