import org.reactivecouchbase.{Timeout, ReactiveCouchbaseDriver}
import org.specs2.mutable._
import play.api.libs.json.Json
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import org.specs2.execute.AsResult
import akka.pattern.after
import org.reactivecouchbase.CouchbaseExpiration._

case class TestValue(value: String, number: Int, l: List[TestValue])

object AtomicUtils {
  implicit val fmt = Json.format[TestValue]
  implicit val urlReader = Json.reads[TestValue]
  implicit val urlWriter = Json.writes[TestValue]
  implicit val ec = ExecutionContext.Implicits.global
  val timeout = 10 seconds
}

class AtomicSpec extends Specification with Tags {
  sequential

  import AtomicUtils._

"""
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
""" in ok

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  val tk = "mylocktestkey_" // + UUID.randomUUID

  "ReactiveCouchbase atomic API" should {

    "delete locked key" in {
      val fut = bucket.delete(tk)
      Await.result(fut, timeout)
      success
    }

    "set key \"" + tk + "\" in default bucket" in {

      val tv = new TestValue("testValue", 42, List())
      val s = bucket.set[TestValue](tk, tv, Duration(2, "hours"))
      Await.result(s, Duration(20000, "millis")).isSuccess must equalTo(true)

    }

    "lock the key \"" + tk + "\" in default bucket" in {

      val f = { x: TestValue =>
        {
          val l = x.l.toList
          val ll = (new TestValue("testValue", l.size, List())) :: l
          val ntv = new TestValue(x.value, x.number, ll)
          ntv
        }
      }
      bucket.atomicUpdate[TestValue](tk, f).onComplete({
        case Success(r) => println("Success of 1 atomic update")
        case Failure(r) => println("Faillure of 1 atomic update")
      })
      bucket.atomicUpdate[TestValue](tk, f).onComplete({
        case Success(r) => println("Success of 2 atomic update")
        case Failure(r) => println("Faillure of 2 atomic update")
      })
      bucket.atomicUpdate[TestValue](tk, f).onComplete({
        case Success(r) => println("Success of 3 atomic update")
        case Failure(r) => println("Faillure of 3 atomic update")
      })
      bucket.atomicUpdate[TestValue](tk, f).onComplete({
        case Success(r) => println("Success of 4 atomic update")
        case Failure(r) => println("Faillure of 4 atomic update")
      })

      bucket.atomicUpdate[TestValue](tk + "-plop", f).onComplete({
        case Success(r) => println("Failure : successfully updated a non existing key O_o")
        case Failure(r) => println("Success : unable to update a non existing key")
      })
      Await.result(after(Duration(6000, "millis"), using = bucket.driver.scheduler())(Future.successful(true)), Duration(7001, "millis"))
    }

    "delete locked key (again)" in {
      val fut = bucket.delete(tk)
      Await.result(fut, timeout)
      success
    }

    "shutdown now" in {
      //Await.result(bucket.flush(), timeout)
      driver.shutdown()
      success
    }
  }
}
