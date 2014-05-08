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

    "shutdown now" in {
      driver.shutdown()
      success
    }
  }
}