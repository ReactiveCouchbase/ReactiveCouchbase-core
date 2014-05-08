import org.reactivecouchbase.{CouchbaseRWImplicits, ReactiveCouchbaseDriver}
import org.reactivecouchbase.CouchbaseRWImplicits.documentAsJsObjectReader
import org.reactivecouchbase.CouchbaseRWImplicits.jsObjectToDocumentWriter
import org.specs2.mutable.{Tags, Specification}
import play.api.libs.json.{JsArray, JsObject, Json}
import scala.concurrent.{ExecutionContext, Await}

class EnumeratorFetchSpec extends Specification with Tags {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucketDefault = driver.bucket("default")

  "ReactiveCouchbase Read API" should {

    "not be able to fetch vales" in {
      val list = Await.result(bucketDefault.fetchValues[JsObject](Seq[String]("key1", "key2")).toList, timeout)
      list.size shouldEqual 0
      success
    }

    "shutdown now" in {
      driver.shutdown()
      success
    }
  }
}