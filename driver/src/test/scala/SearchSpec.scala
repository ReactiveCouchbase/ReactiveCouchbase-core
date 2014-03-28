import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query}
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable._
import play.api.libs.json.Json
import scala.concurrent._
import scala.concurrent.duration._

class SearchSpec extends Specification with Tags {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase search API" should {

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

    "find people youger than 42" in {
      val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setRange(ComplexKey.of(0: java.lang.Integer), ComplexKey.of(41: java.lang.Integer))
      Await.result(bucket.searchValues[Person]("persons", "by_age")(query).toList.map { list =>
        list.map { person =>
          if (person.age >= 42) failure(s"$person is older than 42")
        }
      }, timeout)
      success
    }

    "find people older than 42" in {
      val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setRange(ComplexKey.of(43: java.lang.Integer), ComplexKey.of(100: java.lang.Integer))
      Await.result(bucket.searchValues[Person]("persons", "by_age")(query).toList.map { list =>
        list.map { person =>
          if (person.age <= 42) failure(s"$person is younger than 42")
        }
      }, timeout)
      success
    }

    "find people named Billy" in {
      val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setRange(ComplexKey.of("Billy"), ComplexKey.of("Billy\uefff"))
      Await.result(bucket.searchValues[Person]("persons", "by_name")(query).toList.map { list =>
        if (list.size != 100) failure(s"No enought persons : ${list.size}")
      }, timeout)
      success
    }

    "find people named Bobby" in {
      val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setRange(ComplexKey.of("Bobby"), ComplexKey.of("Bobby\uefff"))
      Await.result(bucket.searchValues[Person]("persons", "by_name")(query).toList.map { list =>
        if (!list.isEmpty) failure("Found people named Bobby")
      }, timeout)
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
      //Await.result(bucket.flush(), timeout)
      driver.shutdown()
      success
    }
  }
}
