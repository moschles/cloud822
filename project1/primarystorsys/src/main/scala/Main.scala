/*
Main for primary storage system.

Maintain a remote, replicated, relational database with clients over a network. 
Expect that all network traffic to this storage system is mediated by a coordinator service.

last mod: Sunday, October 15, 2023 8:39:11 PM
*/
package com
import scala.io.Source
import java.io.{File,BufferedWriter,FileWriter}
import java.nio.charset.Charset
import collection.mutable.Queue 
import com.core.RemoteReplicatedRelationaldb

//* * *// 
object Main extends App {
   
  val userclargs: Option[String] =
    args.length match {
      case 0 => {
        println("Must specify the replica name on the command line.")
        println("I don't now which replica this is. JAR exiting." )
        sys.exit(-1) 
        None
      }
      case _ => Some(args(0))  
    }

  println("server loading cloud settings ...") 
  val contentSettings = 
  try { 
    open("cloudsettings").read().mkString(",").split(",").toList
  } catch { 
    case e:Throwable => {
      println("You must include a cloudsettings file in working directory. JAR failed to load.")
      sys.exit(-1) 
  } } 

  val silencearg:List[String] = parse_by( "silent" , contentSettings )
  val silence:Boolean = if( silencearg.isEmpty ) {
    false 
  } else {
    println("running in silent mode")
    true
  }

  
  val replicablackbook = try{parse_replicas(contentSettings)} catch {
    case e:Throwable => {
      println("cloudsettings file has wrong format. JAR failed to load.")
      properly_formatted_cloudesettings()
      sys.exit(-1) 
    } } 


  val thisReplica:(String,String,Int) = matching_replica( userclargs.get, replicablackbook )
  thisReplica._1 match {
    case "no match" => {
      println("Name does not match any replica names in cloudsettings. JAR exiting.") 
      sys.exit(-1) 
    }
    case _ => {
      println("This server replica%s".format(thisReplica._1) )
    }
  }
  val name:String = thisReplica._1
  val port:Int = thisReplica._3
   
  

  val primarystorage = new RemoteReplicatedRelationaldb( name, port, true , silence )
  (primarystorage.enabled()) match {
    case Some(i) => primarystorage.wait_server( )
    case _ => println("Replica failed to create an in-memory storage system. JAR terminated.")
  } 

  // // 
  def matching_replica( userclarg: String, book:List[(String,String,Int)] ):(String,String,Int) ={
    val cmdlname = userclarg.replace("replica"," ").trim() 
    val matchReplica:List[(String,String,Int)] = book.filter(_._1==cmdlname)
    if( matchReplica.isEmpty ) {
      ("no match","ipv4",8)
    } else {
      matchReplica.head
    }
  }

  // // 
  def open(path: String) = new File(path)

  // // 
  implicit class RichFile(file: File) {
    def read() = Source.fromFile(file).getLines()
  }

   // // // 
  def parse_replicas(store:List[String]):List[(String,String,Int)] = {
    val replSettings = contentSettings.filter(_ contains "replica")
    println( "found %d replicas".format( replSettings.length ))
    val repltup:List[(String,String,Int)] = for( elt<-replSettings ) yield {
      val ch = elt.replace("replica"," ").trim()
      val tok = ch.split(":").toList
      (tok(0),tok(1),tok(2).toInt)
    }
    for(elt<-repltup) {
      println("  replica%s , ipv4=%s  port %d".format( elt._1, elt._2,elt._3)  )
    }
    repltup
  }

  // // // 
  def properly_formatted_cloudesettings() = {
    val fname = "cloudsettings.form"
    println("A properly-formatted example cloudsettings file written to %s".format(fname) ) 
val exform ="""replica 00:127.0.0.1:8177  
replica 01:127.0.0.1:8179
replica 02:127.0.0.1:8181
client F8E0-AB43:8
client 8BB7-90C9:8  
recess 2000
timeout 30"""
    val file = new File( fname )
    val bw = new BufferedWriter(new FileWriter(file,Charset.forName("UTF8")))
    bw.write( exform ) 
    bw.close()
  }

  // // Given a list of strings from a settings file, store.
  //  return the matching value for the setting, want 
  def parse_by(want:String,store:List[String]):List[String] = {
    val wmat = store.filter(  _ contains want  ).toList
    if( wmat.isEmpty ) {
      wmat
    } else {
      List( wmat.head.replace(want," ").trim() )    
    }
  } 
}
