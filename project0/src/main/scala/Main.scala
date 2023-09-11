package com
import scala.io.Source
import java.io.{File,BufferedWriter,FileWriter}
import java.nio.charset.Charset
import collection.mutable.Queue 
import com.core.Pingc
import com.core.Pongs

//* * *// 
object Main extends App {

  val userclargs: Option[String] =
    args.length match {
      case 0 => None
      case _ => Some(args(0))  
    }


  val functionalMain :String = userclargs match {
    case None => "client" 
    case Some(uc) => {if(uc=="pong") "server" else "client"}
  }

  val contentSettings = open("cloudsettings").read().mkString(",").split(",").toList
  val port  = parse_by( "port",contentSettings).toInt 
  val count = parse_by("count",contentSettings).toInt
  val clientip = parse_by("client",contentSettings)
  val serverip = parse_by("server",contentSettings)
  val recess = parse_by("recess",contentSettings).toInt
  val timeout = parse_by("timeout",contentSettings).toInt
  println( "count ", count )
  println( "recess (ms)", recess)
  println( "timeout (sec)", timeout)
  println( "clientip ", clientip )
  println("serverip ", serverip ) 
  println( "port ",port )


  functionalMain match {
    
    //
    case "client" => {
      println("jar will perform as client.")
      var Q=Queue[String]()
      for( p <- 1 to count ) {
        println(" ")
        Thread.sleep(recess)
        print( s"$p   ") 
        var pingc = new Pingc( serverip , port , count, timeout )
        var responseTime:Option[Double] = (pingc.enabled()) match {
          case Some(i) => pingc.send_ping( )
          case _ => println("abort from earlier failure."); None;
        }
        responseTime match {
          case Some(r) => Q += (s"$p " + f"$r%.7f") 
          case None => Q += "NaN"
         }
      }

      try {
        val file = new File(s"results$count" )
        val bw = new BufferedWriter(new FileWriter(file,Charset.forName("UTF8")))
        bw.write( Q.mkString("\n") )
        bw.close()
      } catch {
        case _ : Exception => println("output file is no longer up-to-date.")
      } finally {
        Q.clear()
      }
        

    }

    // 
    case "server" => {
      println("jar will perform as server.")
      val pongs = new Pongs( port )
      (pongs.enabled()) match {
        case Some(i) => pongs.wait_ping( )
        case _ => println("abort from earlier failure.")
      }
    }
  }
 
  // // Given a list of strings from a settings file, store.
  //  return the matching value for the setting, want 
  def parse_by(want:String,store:List[String]):String = {
    store.filter(  _ contains want  ).head.replace(want," ").trim()  
  } 

  // // 
  def open(path: String) = new File(path)

  // // 
  implicit class RichFile(file: File) {
    def read() = Source.fromFile(file).getLines()
  }
}