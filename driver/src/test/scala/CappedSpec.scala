import java.util.concurrent.atomic.AtomicInteger
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable._
import play.api.libs.iteratee.{Enumerator, Iteratee}
import play.api.libs.json.Json
import scala.concurrent._
import scala.concurrent.duration._
import akka.pattern.after

class CappedSpec extends Specification with Tags {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val b = driver.bucket("default")
  val bucket = driver.cappedBucket("default", 100, false)
  val personsAndKeys1 = List(
    ("person-1", Person("John", "Doe", 42)),
    ("person-2", Person("Jane", "Doe", 42)),
    ("person-3", Person("Billy", "Doe", 42)),
    ("person-4", Person("Bobby", "Doe", 42))
  )
  val personsAndKeys2 = List(
    ("person-5", Person("John", "Doe", 42)),
    ("person-6", Person("Jane", "Doe", 42)),
    ("person-7", Person("Billy", "Doe", 42)),
    ("person-8", Person("Bobby", "Doe", 42))
  )
  val personsAndKeys3 = List(
    ("person-9", Person("John", "Doe", 42)),
    ("person-10", Person("Jane", "Doe", 42)),
    ("person-11", Person("Billy", "Doe", 42)),
    ("person-12", Person("Bobby", "Doe", 42))
  )
  val personsAndKeys4 = List(
    ("person-13", Person("John", "Doe", 42)),
    ("person-14", Person("Jane", "Doe", 42)),
    ("person-15", Person("Billy", "Doe", 42)),
    ("person-16", Person("Bobby", "Doe", 42))
  )
  val counter = new AtomicInteger(0)

  "ReactiveCouchbase capped API" should {

    "insert and read some stuff" in {

      bucket.tail[Person]().apply(Iteratee.foreach { slug =>
        b.logger.info(s"from query : $slug")
        counter.incrementAndGet()
      })
      counter.get().mustEqual(0)
      bucket.insertStream(Enumerator.enumerate(personsAndKeys1))
      Await.result(after(Duration(2000, "millis"), using = b.driver.scheduler())(Future.successful(true)), Duration(2500, "millis"))
      counter.get().mustEqual(4)
      bucket.insertStream(Enumerator.enumerate(personsAndKeys1))
      Await.result(after(Duration(2000, "millis"), using = b.driver.scheduler())(Future.successful(true)), Duration(2500, "millis"))
      counter.get().mustEqual(8)
      bucket.insertStream(Enumerator.enumerate(personsAndKeys2))
      Await.result(after(Duration(2000, "millis"), using = b.driver.scheduler())(Future.successful(true)), Duration(2500, "millis"))
      counter.get().mustEqual(12)
      bucket.insertStream(Enumerator.enumerate(personsAndKeys3))
      Await.result(after(Duration(2000, "millis"), using = b.driver.scheduler())(Future.successful(true)), Duration(2500, "millis"))
      counter.get().mustEqual(16)
      for (i <- 17 to 100) {
        bucket.insert(s"person-$i", Person("Jane", "Doe", i))
      }
      Await.result(after(Duration(2000, "millis"), using = b.driver.scheduler())(Future.successful(true)), Duration(2500, "millis"))
      counter.get().mustEqual(100)
      success
    }

    "delete some data" in {
      for (i <- 1 to 100) {
        Await.result(b.delete(s"person-$i"), timeout)
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
