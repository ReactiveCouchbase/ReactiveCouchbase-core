import org.reactivecouchbase.{ReactiveCouchbaseDriver, Couchbase}
import org.specs2.mutable._
import play.api.libs.json.Json
import scala.concurrent._
import scala.concurrent.duration._

case class Person(name: String, surname: String, age: Int)

object Utils {
  implicit val personFmt = Json.format[Person]
  implicit val ec = ExecutionContext.Implicits.global
  val timeout = 10 seconds
}

class CrudSpec extends Specification with Tags {
  sequential

  import Utils._

  println(
    """

====================================================================================================

You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...

====================================================================================================

    """)

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase" should {

    "not be able to find some data" in {
      val fut = bucket.get[Person]("person-key1").map { opt =>
        if (!opt.isEmpty) {
          failure("Found John Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "insert some data" in {
      val expectedPerson = Person("John", "Doe", 42)
      val fut = bucket.set[Person]("person-key1", expectedPerson).map { status =>
        if (!status.isSuccess) {
          failure("Cannot persist John Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "fetch some data" in {
      val expectedPerson = Person("John", "Doe", 42)
      val fut = bucket.get[Person]("person-key1").map { opt =>
        if (opt.isEmpty) {
          failure("Cannot fetch John Doe")
        }
        val person = opt.get
        person.mustEqual(expectedPerson)
      }
      Await.result(fut, timeout)
      success
    }

    "update some data" in {
      val expectedPerson = Person("Jane", "Doe", 42)
      val fut = bucket.replace[Person]("person-key1", expectedPerson).map { status =>
        if (!status.isSuccess) {
          failure("Cannot persist Jane Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "fetch some data (again)" in {
      val expectedPerson = Person("Jane", "Doe", 42)
      val fut = bucket.get[Person]("person-key1").map { opt =>
        if (opt.isEmpty) {
          failure("Cannot fetch Jane Doe")
        }
        val person = opt.get
        person.mustEqual(expectedPerson)
      }
      Await.result(fut, timeout)
      success
    }

    "delete some data" in {
      val fut = bucket.delete("person-key1").map { status =>
        if (!status.isSuccess) {
          failure("Cannot delete John Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "not be able to find some data (again)" in {
      val fut = bucket.get[Person]("person-key1").map { opt =>
        if (!opt.isEmpty) {
          failure("Found John Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "add some data" in {
      val expectedPerson = Person("Billy", "Doe", 42)
      val fut = bucket.add[Person]("person-key2", expectedPerson).map { status =>
        if (!status.isSuccess) {
          failure("Cannot persist Billy Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "fetch some data (again)" in {
      val expectedPerson = Person("Billy", "Doe", 42)
      val fut = bucket.get[Person]("person-key2").map { opt =>
        if (opt.isEmpty) {
          failure("Cannot fetch Billy Doe")
        }
        val person = opt.get
        person.mustEqual(expectedPerson)
      }
      Await.result(fut, timeout)
      success
    }

    "add some data (again)" in {
      val expectedPerson = Person("Bobby", "Doe", 42)
      val fut = bucket.add[Person]("person-key2", expectedPerson).map { status =>
        if (status.isSuccess) {
          failure(s"Can persist Bobby Doe but shouldn't : ${status.getMessage}")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "fetch some data (yes, again)" in {
      val expectedPerson = Person("Billy", "Doe", 42)
      val fut = bucket.get[Person]("person-key2").map { opt =>
        if (opt.isEmpty) {
          failure("Cannot fetch Billy Doe")
        }
        val person = opt.get
        person.mustEqual(expectedPerson)
      }
      Await.result(fut, timeout)
      success
    }

    "delete some data (again)" in {
      val fut = bucket.delete("person-key2").map { status =>
        if (!status.isSuccess) {
          failure("Cannot delete Billy Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "not be able to find some data (again)" in {
      val fut = bucket.get[Person]("person-key2").map { opt =>
        if (!opt.isEmpty) {
          failure("Found Billy or Bobby Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "shutdown now" in {
      driver.shutdown()
      success
    }
  }
}
