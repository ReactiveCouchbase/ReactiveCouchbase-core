import org.specs2.mutable.Specification
import org.specs2.time.NoTimeConversions
import scala.compat.Platform
import scala.concurrent.duration._
import org.reactivecouchbase.CouchbaseExpiration._

class CouchbaseExpirationSpec extends Specification with NoTimeConversions {
  "CouchbaseExpiration" should {
    "use TTL in seconds for expiration <=30 days" in {
      val expiration = 30.days

      ((expiration: CouchbaseExpirationTiming): Int) === expiration.toSeconds

    }
    "use unix epoch for expiration >30 days" in {
      val expiration = 30.days + 1.second

      val expectedUnixEpoch = ((Platform.currentTime + expiration.toMillis) / 1000).toInt
      ((expiration: CouchbaseExpirationTiming): Int) must be ~ (expectedUnixEpoch +/- 3)
    }
  }
}
