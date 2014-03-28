import java.util.concurrent.TimeUnit
import org.reactivecouchbase.crud.ReactiveCRUD
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable.{Tags, Specification}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import Utils._

object BeerCRUD extends ReactiveCRUD[Beer] {
  val driver = ReactiveCouchbaseDriver()
  def bucket = driver.bucket("default")
}

class ReactiveCrudSpec extends Specification with Tags {
  sequential

  val timeout = Duration(10, TimeUnit.SECONDS)

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  "ReactiveCouchbase ReactiveCRUD API" should {

    "create some view" in {
      Await.result(BeerCRUD.bucket.createDesignDoc("testbeers",
        """
          | {
          |     "views":{
          |        "by_name": {
          |            "map": "function (doc, meta) { emit(doc.name, null); } "
          |        },
          |        "by_price": {
          |            "map": "function (doc, meta) { emit(doc.price, null); } "
          |        },
          |        "by_from": {
          |            "map": "function (doc, meta) { emit(doc.from, null); } "
          |        }
          |     }
          | }
        """.stripMargin), timeout)
      success
    }

    "insert a lot of data" in {
      for(i <- 0 to 99) {
        Await.result(BeerCRUD.insert(Beer(s"beer--$i", s"Duff-$i", 2.0,s"Springfield-$i")), timeout)
      }
      success
    }

    "delete all data" in {
      Await.result(BeerCRUD.bucket.deleteDesignDoc("testbeers"), timeout)
      for(i <- 0 to 99) {
        Await.result(BeerCRUD.delete(s"beer--$i"), timeout)
      }
      success
    }
    
    "shutdown now" in {
      BeerCRUD.driver.shutdown()
      success
    }
  }
}