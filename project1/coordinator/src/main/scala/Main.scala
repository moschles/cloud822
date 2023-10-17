/*
Main for Coordinator. 

last mod:  
Monday, October 9, 2023 1:25:22 AM

*/

package com

// Java //
import java.io.{File,BufferedWriter,FileWriter}
import java.nio.charset.Charset
import java.util.concurrent.Executors
 
// Scala // 
import scala.io.Source
import scala.concurrent.ExecutionContext
import collection.mutable.Queue

// Locals // 
import com.core.Coordinator
import com.core.Benchmark



//* * *// 
object Main extends App 
{
  // Threadpool at the highest level of the JVM. 
  implicit val extCONext = new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool( 64 );
    override def reportFailure(cause: Throwable): Unit = {};
    override def execute(runnable: Runnable): Unit = threadPool.submit(runnable);
    def shutdown_pool() = threadPool.shutdown();
  }

  val useOneClient:Boolean = false // for testing 

  val contentSettings = 
  try { 
    open("cloudsettings").read().mkString(",").split(",").toList
  } catch { 
    case e:Throwable => {
      println("You must include a cloudsettings file in working directory. JAR failed to load.")
      sys.exit(-1) 
  } } 

  val recess = parse_by("recess",contentSettings).toInt
  val timeout = parse_by("timeout",contentSettings).toInt
  println( "recess (ms)", recess)
  println( "timeout (sec)", timeout)

  val clientfullbook  = try {   parse_clients( contentSettings) } catch { 
    case e:Throwable => {
      println("cloudsettings file has wrong format. JAR failed to load.")
      properly_formatted_cloudesettings()
      sys.exit(-1) 
  } }

  val clientblackbook = if (useOneClient) {List( clientfullbook.head )} else {clientfullbook}
  
  val replicablackbook = try{parse_replicas(contentSettings)} catch {
    case e:Throwable => {
      println("cloudsettings file has wrong format. JAR failed to load.")
      properly_formatted_cloudesettings()
      sys.exit(-1) 
    } } 

  //.

  val B = new Benchmark( clientblackbook , replicablackbook  , recess, timeout , extCONext ) 
  B.time_trials( ) 
  B.store_results( ) 

  extCONext.shutdown_pool( )

  // // Given a list of strings from a settings file, store.
  //  return the matching value for the setting, want 
  def parse_by(want:String,store:List[String]):String = {
    store.filter(  _ contains want  ).head.replace(want," ").trim()  
  } 

  // // // 
  def parse_clients(  store:List[String]   ):List[(String,Int)] = {
    val clientSettings = contentSettings.filter(_ contains "client")
    println( "found %d client(s)".format( clientSettings.length ))
    val clienttup:List[(String,Int)] = for( elt<-clientSettings ) yield {
      val ch = elt.replace("client"," ").trim()
      val tok = ch.split(":").toList
      (tok(0),tok(1).toInt)
    }
    for(elt<-clienttup) {
      println("clientUUID= %s , count=%d".format( elt._1, elt._2) )
    }
    clienttup
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
      println("replica%s , ipv4=%s  port %d".format( elt._1, elt._2,elt._3)  )
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

  // // 
  def open(path: String) = new File(path)

  // // 
  implicit class RichFile(file: File) {
    def read() = Source.fromFile(file).getLines()
  }
}