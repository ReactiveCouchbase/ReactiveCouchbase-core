package samples

// first import the implicit execution context
import net.spy.memcached.ops.OperationStatus
import scala.concurrent.ExecutionContext.Implicits.global
import org.reactivecouchbase.ReactiveCouchbaseDriver
import com.couchbase.client.protocol.views.Query
import play.api.libs.iteratee.Iteratee

// import the implicit JsObject reader and writer
import org.reactivecouchbase.CouchbaseRWImplicits.documentAsJsObjectReader
import org.reactivecouchbase.CouchbaseRWImplicits.jsObjectToDocumentWriter
import scala.concurrent.Future
import play.api.libs.json._

object Application1 extends App {

  // get a driver instance driver  
  val driver = ReactiveCouchbaseDriver()
  // get the default bucket  
  val bucket = driver.bucket("default")

  // creates a JSON document
  val document = Json.obj(
    "name" -> "John",
    "surname" -> "Doe",
    "age" -> 42,
    "address" -> Json.obj(
      "number" -> "221b",
      "street" -> "Baker Street",
      "city" -> "London"
    )
  )
  // persist the JSON doc with the key 'john-doe', using implicit 'jsObjectToDocumentWriter' for serialization
  bucket.set[JsObject]("john-doe", document).onSuccess {
    case status => println(s"Operation status : ${status.getMessage}")
  }

  // shutdown the driver
  driver.shutdown()
}

case class Address(number: String, street: String, city: String)
case class Person(name: String, surname: String, age: Int, address: Address)

object Application2 extends App {

  // get a driver instance driver
  val driver = ReactiveCouchbaseDriver()
  // get the default bucket
  val bucket = driver.bucket("default")

  // provide implicit Json formatters
  implicit val addressFmt = Json.format[Address]
  implicit val personFmt = Json.format[Person]

  // creates a JSON document
  val document = Person("John", "Doe", 42, Address("221b", "Baker Street", "London"))

  // persist the Person instance with the key 'john-doe', using implicit 'personFmt' for serialization
  bucket.set[Person]("john-doe", document).onSuccess {
    case status => println(s"Operation status : ${status.getMessage}")
  }

  // shutdown the driver
  driver.shutdown()
}

object Application3 extends App {

  // get a driver instance driver
  val driver = ReactiveCouchbaseDriver()
  // get the default bucket
  val bucket = driver.bucket("default")

  // provide implicit Json formatters
  implicit val addressFmt = Json.format[Address]
  implicit val personFmt = Json.format[Person]

  // get the Person instance with the key 'john-doe', using implicit 'personFmt' for deserialization
  bucket.get[Person]("john-doe").map { opt =>
    println(opt.map(person => s"Found John : ${person}").getOrElse("Cannot find object with key 'john-doe'"))
  }

  // shutdown the driver
  driver.shutdown()
}

object Application4 extends App {

  // get a driver instance driver
  val driver = ReactiveCouchbaseDriver()
  // get the default bucket
  val bucket = driver.bucket("default")

  // search all docs from the view 'brewery_beers'
  // then enumerate it and print each document on the fly
  // streaming style
  bucket.searchValues[JsObject]("beers", "brewery_beers")(new Query().setIncludeDocs(true))
        .enumerate.apply(Iteratee.foreach { doc =>
    println(s"One more beer for the road  : ${Json.prettyPrint(doc)}")
  })

  // search the whole list of docs from the view 'brewery_beers'
  val futureList: Future[List[JsObject]] =
    bucket.searchValues[JsObject]("beers", "brewery_beers")(new Query().setIncludeDocs(true)).toList
  // when the query is done, run over all the docs and print them
  futureList.map { list =>
    list.foreach { doc =>
      println(s"One more beer for the road : ${Json.prettyPrint(doc)}")
    }
  }
  // shutdown the driver
  driver.shutdown()
}
