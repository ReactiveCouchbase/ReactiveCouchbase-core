import java.util.concurrent.TimeUnit
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable._
import play.api.libs.json.Json
import scala.concurrent._
import scala.concurrent.duration._

class BulkSpec extends Specification with Tags {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase and its BulkAPI" should {

    "not be able to find some data" in {
      val fut = bucket.get[Person]("person-key1").map { opt =>
        if (!opt.isEmpty) {
          failure("Found John Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "insert some John DOE" in {
      val expectedPerson = Person("John", "Doe", 42)
      val fut = bucket.set[Person]("person-key1", expectedPerson).map { status =>
        if (!status.isSuccess) {
          failure("Cannot persist John Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "insert some Jane DOE" in {
      val expectedPerson = Person("Jane", "Doe", 42)
      val fut = bucket.set[Person]("person-key2", expectedPerson).map { status =>
        if (!status.isSuccess) {
          failure("Cannot persist Jane Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "insert some Billy DOE" in {
      val expectedPerson = Person("Billy", "Doe", 42)
      val fut = bucket.set[Person]("person-key3", expectedPerson).map { status =>
        if (!status.isSuccess) {
          failure("Cannot persist Billy Doe")
        }
      }
      Await.result(fut, timeout)
      success
    }

    "fetch some data" in {
      val expectedPersons = List(
        Person("John", "Doe", 42),
        Person("Jane", "Doe", 42),
        Person("Billy", "Doe", 42)
      )
      var seq = List[Future[Seq[Option[Person]]]]()
      for (i <- 0 to 100) {
        val fut: Future[List[Option[Person]]]  = Future.sequence(List(
          bucket.get[Person]("person-key1"),
          bucket.get[Person]("person-key2"),
          bucket.get[Person]("person-key3")
        )).map { list =>
          //println("Got something : " + list)
          if (list == null || list.isEmpty) failure("List should not be empty ...")
          list.foreach { opt =>
            if (opt.isEmpty) {
              failure("Cannot fetch John Doe")
            }
            val person = opt.get
            expectedPersons.contains(person).mustEqual(true)
          }
          list
        }
        seq = seq :+ fut
      }
      Await.result(Future(seq), Duration(120, TimeUnit.SECONDS))
      //Await.result(fut, timeout)
      success
    }

    "delete some data" in {
      val fut = Future.sequence(Seq(
        bucket.delete("person-key1"),
        bucket.delete("person-key2"),
        bucket.delete("person-key3")
      ))
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

class CrudSpec extends Specification with Tags {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

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

    "fail on non string doc" in {
      val key = "nonString"
      bucket.couchbaseClient.set(key, 0)
      val fut = bucket.get[Person](key)
      Await.result(fut, timeout) must beNone //throwAn[IllegalStateException]
      Await.result(bucket.delete(key), timeout)
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
      //Await.result(bucket.flush(), timeout)
      driver.shutdown()
      success
    }
  }
}