import org.scalatest.flatspec._ 
import org.scalatest.matchers.should._
import org.scalatest.BeforeAndAfterAll

//
import com.core.ReplicatedRelationaldb
import com.core.RemoteReplicatedRelationaldb

class RRdbSuite extends AnyFlatSpec with Matchers with BeforeAndAfterAll
{
    val clid="client"

    "ReplicatedRelationaldb" should "accept DELETE on non-existent entries" in {
        val rrdb = new ReplicatedRelationaldb()
        rrdb.DELETE(1234)
        rrdb.DELETE( 9 )
        val sz = 23
        sz shouldEqual (23)
    }

    "ReplicatedRelationaldb" should "GET() a PUT() properly" in {
        val rrdb = new ReplicatedRelationaldb()
        rrdb.PUT(1234,"foo",clid)
        val h = rrdb.GET(1234)
        h.get._1 shouldEqual ("foo")
    }

    "ReplicatedRelationaldb" should "accept GET() on non-existent entry" in {
        val rrdb = new ReplicatedRelationaldb()
        rrdb.PUT(1234,"foo",clid)
        val ne = rrdb.GET(7)
        val h = rrdb.GET(1234)
        ne match {
            case None=> { 1 shouldEqual(1) }
            case Some(x)=>{ 1 shouldEqual(11) }
        }

        val ngg = rrdb.GET(-8)
        ngg match {
            case None=> { 1 shouldEqual(1) }
            case Some(x)=>{ 1 shouldEqual(11) }
        }
    }

    "ReplicatedRelationaldb" should "update version on PUT()" in {
        val rrdb = new ReplicatedRelationaldb()
        rrdb.PUT(76,"waven",clid)
        rrdb.PUT(76,"dash",clid)
        rrdb.PUT(76,"glows",clid)
        rrdb.PUT(76,"under",clid)
        val h = rrdb.GET(76)
        h.get._1 shouldEqual ("under")
        h.get._2 shouldEqual(3)
    }

    "ReplicatedRelationaldb" should "DELETE() after PUT()" in {
        val rrdb = new ReplicatedRelationaldb()
        rrdb.PUT(76,"waven",clid)
        rrdb.PUT(76,"dash", clid)
        rrdb.PUT(76,"glows", clid)
        rrdb.PUT(76,"under", clid)
        rrdb.DELETE(76)
        val h = rrdb.GET(76)
        h match {
            case None=> { 1 shouldEqual(1) }
            case Some(x)=>{ 1 shouldEqual(11) }
        }
    }

    "ReplicatedRelationaldb" should "survive multiple DELETE()" in {
        val rrdb = new ReplicatedRelationaldb()
        rrdb.PUT(76,"waven",clid)
        rrdb.DELETE(76)
        rrdb.PUT(76,"dash", clid)
        rrdb.DELETE(76)
        rrdb.PUT(76,"glows", clid)
        rrdb.DELETE(76)
        rrdb.PUT(76,"under", clid)
        val h = rrdb.GET(76)
        h match {
            case None=> { 1 shouldEqual(11) }
            case Some(x)=>{ x._1 shouldEqual("under");  x._3 shouldEqual(clid) }
        }
    }

    "ReplicatedRelationaldb" should "overwrite client UUID" in {
        val rrdb = new ReplicatedRelationaldb()
        rrdb.PUT(99,"waven",clid)
        rrdb.PUT(99,"dash", "FA9BE")
        val h = rrdb.GET(99)
        h match {
            case None=> { 1 shouldEqual(11) }
            case Some(x)=>{ x._3 shouldEqual("FA9BE") }
        }
    }

    "ReplicatedRelationaldb" should "update logical clock" in {
        val rrdb = new ReplicatedRelationaldb()
        rrdb.PUT(99,"waven",clid)
        val gg = rrdb.GET(99)
        rrdb.DELETE(99)
        rrdb.DELETE(99) 
        val clk = rrdb.clocks
        clk._1 shouldEqual(4)
        clk._2 shouldEqual(1)
        clk._3 shouldEqual(2) 
    }

    "RemoteReplicatedRelationaldb" should "parse PUT without crashing" in {
        val rrrdb = new RemoteReplicatedRelationaldb( "00",8186, false , false)
        val res = rrrdb.parser_test("PUT|33|foo|F8E0-AB43|1")
        println(res) 
    }

    "RemoteReplicatedRelationaldb" should "parse GET without crashing" in {
        val rrrdb = new RemoteReplicatedRelationaldb( "00", 8186, false , false )
        val xxx = rrrdb.parser_test("PUT|33|foo|F8E0-AB43|1")
        val res = rrrdb.parser_test("GET|33|qwehrjlk|8AB1-9009|12")
        println(res) 
    }

    "RemoteReplicatedRelationaldb" should "parse DEL without crashing" in {
        val rrrdb = new RemoteReplicatedRelationaldb( "00", 8186, false, false )
        val xxx = rrrdb.parser_test("PUT|33|foo|F8E0-AB43|1")
        val res = rrrdb.parser_test("DEL|33|foo|F8E0-AB43|2")
        println(res) 
    }
    
}

