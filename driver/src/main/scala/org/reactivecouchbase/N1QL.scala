package org.reactivecouchbase

import scala.concurrent._
import play.api.libs.iteratee._
import play.api.libs.json._
import com.ning.http.client.{Response, AsyncCompletionHandler}
import org.reactivecouchbase.client.ReactiveCouchbaseException

/**
 * Container to run N1QL query against a Couchbase Server. Uses it's own HTTP client from CouchbaseBucket
 *
 * @param bucket the bucket containing an HTTP client. It could be any CouchbaseBucket instance for any targeted actual bucket
 * @param query the N1QL query you want to run
 * @param host the host on which N1QL server is running
 * @param port the port of the N1QL server
 */
class N1QLQuery(bucket: CouchbaseBucket, query: String, host: String, port: String) {
  /**
   * The N1QL URL for querying
   */
  val url = s"http://${host}:${port}/query"

  /**
   *
   * Stream the content of the query result as JsObjects
   *
   * @param ec the execution context in which the Enumerator is executed the execution context in which the Enumerator is executed
   * @return the future JsObject Enumerator
   */
  def enumerateJson(implicit ec: ExecutionContext): Future[Enumerator[JsObject]] = {
    toJsArray(ec).map { arr =>
      Enumerator.enumerate(arr.value) &> Enumeratee.map[JsValue](_.as[JsObject])
    }
  }

  /**
   *
   * Stream the content of the query result as instances of T
   *
   * @param r Json reader to transform raw Json into Scala instances
   * @param ec the execution context in which the Enumerator is executed
   * @tparam T the type of object used by deserialization
   * @return  the future T enumerator
   */
  def enumerate[T](implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    enumerateJson(ec).map { e =>
      e &> Enumeratee.map[JsObject](r.reads(_)) &> Enumeratee.collect[JsResult[T]] { case JsSuccess(value, _) => value }
    }
  }

  /**
   *
   * Stream the content of the query result as JsObjects
   *
   * @param ec the execution context in which the Enumerator is executed
   * @return the Enumerator of JsObject instances
   */
  def asJsonEnumerator(implicit ec: ExecutionContext): Enumerator[JsObject] = {
    Enumerator.flatten(enumerateJson(ec))
    //Concurrent.unicast[JsObject](onStart = c => enumerateJson(ec).map(_(Iteratee.foreach[JsObject](c.push).map(_ => c.eofAndEnd()))))
  }

  /**
   *
   * Stream the content of the query result as instances of T
   *
   * @param r Json reader to transform raw Json into Scala instances
   * @param ec the execution context in which the Enumerator is executed
   * @tparam T the type of object used by deserialization
   * @return the Enumerator of T instances
   */
  def asEnumerator[T](implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.flatten(enumerate[T](r, ec))
    //Concurrent.unicast[T](onStart = c => enumerate[T](r, ec).map(_(Iteratee.foreach[T](c.push).map(_ => c.eofAndEnd()))))
  }

  /**
   *
   * Transform the query result as a JsArray
   * 
   * @param ec the execution context in which the Future is executed
   * @return the future JsArray
   */
  def toJsArray(implicit ec: ExecutionContext): Future[JsArray] = {
    val result = Promise[JsValue]()
    bucket.httpClient.preparePost(url).addQueryParameter("q", query).execute(new AsyncCompletionHandler[Response]() {
      override def onCompleted(response: Response) = {
        result.success(Json.parse(response.getResponseBody))
        response
      }
      override def onThrowable(t: Throwable) = {
        result.failure(t)
      }
    })
    result.future.map { response =>
      (response \ "resultset").as[JsArray]
    }
  }

  /**
   *
   * Transform the query result as a List of T instances
   *
   * @param r Json reader to transform raw Json into Scala instances
   * @param ec the execution context in which the Enumerator is executed
   * @tparam T the type of object used by deserialization
   * @return the future List of T instances
   */
  def toList[T](implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    enumerate[T](r, ec).flatMap(_(Iteratee.getChunks[T]).flatMap(_.run))
  }

  /**
   *
   * Fetch the optional head of a query result
   *
   * @param r Json reader to transform raw Json into Scala instances
   * @param ec the execution context in which the Enumerator is executed
   * @tparam T the type of object used by deserialization
   * @return the future Optional head of the query result
   */
  def headOption[T](implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    enumerate[T](r, ec).flatMap(_(Iteratee.head[T]).flatMap(_.run))
  }
}

/**
 * The N1QL public API for users
 */
object CouchbaseN1QL {

  /**
   *
   * Creates a N1QL query
   *
   * @param bucket the targeted bucket
   * @param query the actual N1QL query written in N1QL query language
   * @param host the host of the N1QL server
   * @param port the port of the N1QL server
   * @return the created N1QLQuery instance
   */
  def N1QL(bucket: CouchbaseBucket, query: String, host: String, port: String): N1QLQuery = {
    new N1QLQuery(bucket, query, host, port)
  }

  /**
   *
   * Creates a N1QL query
   *
   * Example :
   * {{{
   *   import scala.concurrent.ExecutionContext.Implicits.global
   *
   *   case class Person(name: String, age: Int)
   *
   *   val driver = ReactiveCouchbaseDriver()
   *   implicit val bucket = driver.bucket("default")
   *   implicit val fmt = Json.format[Person]
   *
   *   val age = 42
   *
   *   N1QL( s""" SELECT fname, age FROM tutorial WHERE age > ${age} """, driver )
   *                                                .toList[Person].map { persons =>
   *     println(s"Persons older than ${age}", persons))
   *   }
   * }}}
   *
   * @param query the actual N1QL query written in N1QL query language
   * @param bucket the targeted bucket
   * @return the created N1QLQuery instance
   */
  def N1QL(query: String)(implicit bucket: CouchbaseBucket): N1QLQuery = {
    val host = bucket.N1QLHost.getOrElse(throw new ReactiveCouchbaseException("Cannot find N1QL host", "Cannot find N1QL host in couchbase.n1ql conf."))
    val port = bucket.N1QLPort.getOrElse(throw new ReactiveCouchbaseException("Cannot find N1QL port", "Cannot find N1QL port in couchbase.n1ql conf."))
    new N1QLQuery(bucket, query, host, port)
  }
}

