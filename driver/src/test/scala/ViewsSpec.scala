import com.couchbase.client.protocol.views.{Stale, Query}
import org.reactivecouchbase.experimental.{TypedViewRow, Views}
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable._
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.Json
import scala.concurrent._

class ViewsSpec extends Specification with Tags {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  implicit val bucket = driver.bucket("default")

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
      Await.result(bucket.view("persons", "by_name").flatMap { view =>
        Future.sequence(Seq(
          Views.query[Person](view, new Query().setStale(Stale.FALSE).setIncludeDocs(true)).flatMap(e => e.apply(Iteratee.foreach[TypedViewRow[Person]]( doc => println(doc))).map(_.run)),
          Views.query[Person](view, new Query().setStale(Stale.FALSE).setIncludeDocs(true)).flatMap(e => e.apply(Iteratee.foreach[TypedViewRow[Person]]( doc => bucket.logger.debug(doc.document.map(Json.stringify).getOrElse("ERROR !!!")))).map(_.run)),
          Views.query[Person](view, new Query().setStale(Stale.FALSE).setIncludeDocs(true)).flatMap(e => e.apply(Iteratee.foreach[TypedViewRow[Person]]( doc => bucket.logger.debug(doc.document.map(Json.stringify).getOrElse("ERROR !!!")))).map(_.run)),
          Views.query[Person](view, new Query().setStale(Stale.FALSE).setIncludeDocs(true)).flatMap(e => e.apply(Iteratee.foreach[TypedViewRow[Person]]( doc => bucket.logger.debug(doc.document.map(Json.stringify).getOrElse("ERROR !!!")))).map(_.run)),
          Views.query[Person](view, new Query().setStale(Stale.FALSE).setIncludeDocs(true)).flatMap(e => e.apply(Iteratee.foreach[TypedViewRow[Person]]( doc => bucket.logger.debug(doc.document.map(Json.stringify).getOrElse("ERROR !!!")))).map(_.run)),
          Views.query[Person](view, new Query().setStale(Stale.FALSE).setIncludeDocs(true)).flatMap(e => e.apply(Iteratee.foreach[TypedViewRow[Person]]( doc => bucket.logger.debug(doc.document.map(Json.stringify).getOrElse("ERROR !!!")))).map(_.run))
        ))
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
      driver.shutdown()
      success
    }
  }
}
