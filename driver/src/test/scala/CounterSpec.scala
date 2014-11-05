import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.reactivecouchbase.CouchbaseRWImplicits.documentAsJsObjectReader
import org.specs2.mutable.{Tags, Specification}
import play.api.libs.iteratee.Enumerator
import play.api.libs.json.JsObject
import scala.concurrent.Await

class CounterSpec extends Specification with Tags {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase Counter API" should {

    "be able to handle integers" in {
      Await.result(bucket.setInt("counter", 2), timeout)
      Await.result(bucket.getInt("counter"), timeout) mustEqual 2
      Await.result(bucket.delete("counter"), timeout)
      success
    }

    "be able to handle long" in {
      Await.result(bucket.setLong("counterLong", 4L), timeout)
      Await.result(bucket.getLong("counterLong"), timeout) mustEqual 4L
      Await.result(bucket.delete("counterLong"), timeout)
      success
    }


    "be able to increment longs" in {
      
    	Await.result( bucket.setLong("incrementLong", 3L), timeout )
    	Await.result( bucket.incrAndGet("incrementLong", 1L), timeout ) mustEqual 4L
    	Await.result( bucket.delete("incrementLong"), timeout )
    	success
      
    }
    
    "be able to increment integers" in {
      
    	Await.result( bucket.setInt("incrementInt",999) , timeout )	
    	Await.result( bucket.incrAndGet("incrementInt", 1), timeout) mustEqual 1000
    	Await.result( bucket.delete("incrementInt"), timeout )
    	success
      
    }

    "be able to decrement longs" in {
      
    	Await.result( bucket.setLong("decrementLong", 3L), timeout )
    	Await.result( bucket.decrAndGet("decrementLong", 1L), timeout ) mustEqual 2L
    	Await.result( bucket.delete("decrementLong"), timeout )
    	success
      
    }
    
    "be able to decrement integers" in {
      
    	Await.result( bucket.setInt("decrementInt",999) , timeout )	
    	Await.result( bucket.decrAndGet("decrementInt", 1), timeout) mustEqual 998
    	Await.result( bucket.delete("decrementInt"), timeout )
    	success
      
    }

    "increment a non existing long key must fail" in {
      
    	Await.result( bucket.delete("incrementNoKey"), timeout )
    	Await.result( bucket.incr("incrementNoKey", 1L), timeout ).mustNotEqual(beTrue)
    	success
      
    }

    "increment a non existing int key must fail" in {
      
    	Await.result( bucket.delete("incrementNoKey"), timeout )
    	Await.result( bucket.incr("incrementNoKey", 1), timeout ).mustNotEqual(beTrue)
    	success
      
    }

    "shutdown now" in {
      driver.shutdown()
      success
    }
  }
}