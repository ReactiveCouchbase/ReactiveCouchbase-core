import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.pattern.after
import org.reactivecouchbase.CouchbaseExpiration._

class AtomicSpec extends Specification with Tags {
  sequential

  import Utils._

"""
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
""" in ok

  "ReactiveCouchbase atomic API" should {

    import Utils._

    val driver = ReactiveCouchbaseDriver()
    val bucket = driver.bucket("default")

    "try to update a non existent key" in {
      val res = bucket.atomicallyUpdate[Person]("idontexists") { person =>
        Future.successful(Person(person.name, person.surname, person.age + 1))
      }
      res.onComplete {
        case Success(s) => bucket.logger.info("success " + s)
        case Failure(f) => bucket.logger.info("failure " + f)
      }
      Await.ready(res, timeout)
      success
    }

    "try to update an existent key" in {

      Await.result(bucket.set("iexist", Person("John", "Doe", 42)), timeout)

      val res = bucket.atomicallyUpdate[Person]("iexist") { person =>
        Future.successful(Person(person.name, person.surname, person.age + 1))
      }
      res.onComplete {
        case Success(s) => bucket.logger.info("success " + s)
        case Failure(f) => bucket.logger.info("failure " + f)
      }
      Await.result(res, timeout)
      Await.result(bucket.delete("iexist"), timeout)
      success
    }

    "shutdown now" in {
      //Await.result(bucket.flush(), timeout)
      driver.shutdown()
      success
    }
  }

  "ReactiveCouchbase atomic API" should {

    val driver = ReactiveCouchbaseDriver()
    val bucket = driver.bucket("default")

    val tk = "mylocktestkey_" // + UUID.randomUUID

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
      bucket.atomicUpdate[TestValue](tk)(f).onComplete({
        case Success(r) => println("Success of 1 atomic update")
        case Failure(r) => println("Faillure of 1 atomic update")
      })
      bucket.atomicUpdate[TestValue](tk)(f).onComplete({
        case Success(r) => println("Success of 2 atomic update")
        case Failure(r) => println("Faillure of 2 atomic update")
      })
      bucket.atomicUpdate[TestValue](tk)(f).onComplete({
        case Success(r) => println("Success of 3 atomic update")
        case Failure(r) => println("Faillure of 3 atomic update")
      })
      bucket.atomicUpdate[TestValue](tk)(f).onComplete({
        case Success(r) => println("Success of 4 atomic update")
        case Failure(r) => println("Faillure of 4 atomic update")
      })

      bucket.atomicUpdate[TestValue](tk + "-plop")(f).onComplete({
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
