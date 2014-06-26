import org.reactivecouchbase.CouchbaseRWImplicits.jsObjectToDocumentWriter
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.{JsValue, JsObject, Json}

import scala.concurrent.{Future, Await}
import scala.util.{Success, Failure}

class NonExistentSpec extends Specification with Tags {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase read API" should {

    "lookup without throwing" in {

      val none : Future[Option[JsValue]] = bucket.get[JsValue]("a dummy that doesn't exists")
      none.onComplete {
        case Success(Some(stuff)) => println(s"Some of $stuff" )
        case Success(None) => println(s"None")
        case Failure(e) => println(s"Got some error : ${e.getMessage}")
      }
      Await.result(none.recover {
        case _ => None
      }, timeout) must beNone

    }

    "shutdown now" in {
      driver.shutdown()
      success
    }
  }
}