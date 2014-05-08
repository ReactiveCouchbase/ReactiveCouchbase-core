import org.reactivecouchbase.{CouchbaseRWImplicits, ReactiveCouchbaseDriver}
import org.reactivecouchbase.CouchbaseRWImplicits.documentAsJsObjectReader
import org.reactivecouchbase.CouchbaseRWImplicits.jsObjectToDocumentWriter
import org.specs2.mutable.{Tags, Specification}
import play.api.libs.iteratee.Enumerator
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

    "not be able to fetch non existing values" in {
      val list = Await.result(bucketDefault.fetchValues[JsObject](Seq[String]("aspoQdxjSUFKiJ5Vzes56Serpjbv78Xz0SDBdbataH26kOJ8I4spAjrJoLmk2Cca", "E46T7OpyQM8c2du63L0GYtNeyA8e1rRMG9WNynEliCMrHZgQOgT4HEgEzvJWynQo")).toList, timeout)
      list.size shouldEqual 0
      success
    }

    "be able to fetch no values" in {
      val list = Await.result(bucketDefault.fetchValues[JsObject](Seq[String]()).toList, timeout)
      list.size shouldEqual 0
      success
    }

    "be able to fetch no values from enumerator" in {
      val list = Await.result(bucketDefault.fetchValues[JsObject](Enumerator.empty[String]).toList, timeout)
      list.size shouldEqual 0
      success
    }

    "shutdown now" in {
      driver.shutdown()
      success
    }
  }
}