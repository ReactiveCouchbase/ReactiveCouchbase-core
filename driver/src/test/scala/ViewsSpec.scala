import java.util.concurrent.TimeUnit
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable._
import scala.concurrent._
import scala.concurrent.duration._

class ViewsSpec extends Specification with Tags {
  sequential

  implicit val ec = ExecutionContext.Implicits.global
  val timeout = Duration(10, TimeUnit.SECONDS)

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase Views API" should {

    "create some view" in {
      Await.result(bucket.createDesignDoc("persons",
        """
          | {
          |     "views":{
          |        "by_name": {
          |            "map": "function (doc, meta) { emit(doc.name, null); } "
          |        },
          |        "by_surname": {
          |            "map": "function (doc, meta) { emit(doc.surname, null); } "
          |        },
          |        "by_age": {
          |            "map": "function (doc, meta) { emit(doc.age, null); } "
          |        }
          |     }
          | }
        """.stripMargin), timeout)
      success
    }

    "insert a lot of data" in {
      for(i <- 0 to 99) {
        Await.result(bucket.set(s"person--$i", Person("Billy", s"Doe-$i", i)), timeout)
      }
      success
    }

    "Check view API" in {

      success
    }

    "delete all data" in {
      Await.result(bucket.deleteDesignDoc("persons"), timeout)
      for(i <- 0 to 99) {
        Await.result(bucket.delete(s"person--$i"), timeout)
      }
      success
    }


    "shutdown now" in {
      driver.shutdown()
      success
    }
  }
}
