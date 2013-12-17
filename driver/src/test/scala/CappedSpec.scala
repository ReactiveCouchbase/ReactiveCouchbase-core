import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable._
import play.api.libs.json.Json
import scala.concurrent._
import scala.concurrent.duration._

object CappedUtils {
  implicit val ec = ExecutionContext.Implicits.global
  val timeout = 10 seconds
}

class CappedSpec extends Specification with Tags {
  sequential

  import CappedUtils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase" should {
    "shutdown now" in {
      //Await.result(bucket.flush(), timeout)
      driver.shutdown()
      success
    }
  }
}
