import play.api.libs.json.Json
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

case class Beer(_id: String, name: String, price:Double, from: String)

case class TestValue(value: String, number: Int, l: List[TestValue])

case class Person(name: String, surname: String, age: Int)

object Utils {
  implicit val beerFmt = Json.format[Beer]
  implicit val testValueFmt = Json.format[TestValue]
  implicit val personFmt = Json.format[Person]
  implicit val ec = ExecutionContext.Implicits.global
  val timeout = 10 seconds
}