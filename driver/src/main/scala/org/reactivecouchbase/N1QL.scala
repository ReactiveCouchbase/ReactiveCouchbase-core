package org.reactivecouchbase

import scala.concurrent._
import play.api.libs.iteratee._
import play.api.libs.json._
import com.ning.http.client.{Response, AsyncCompletionHandler, AsyncHttpClient, AsyncHttpClientConfig}
import org.reactivecouchbase.client.ReactiveCouchbaseException

class N1QLQuery(query: String, host: String, port: String) {
  val url = s"http://${host}:${port}/query"
  def enumerateJson(implicit ec: ExecutionContext): Future[Enumerator[JsObject]] = {
    toJsArray(ec).map { arr =>
      Enumerator.enumerate(arr.value) &> Enumeratee.map[JsValue](_.as[JsObject])
    }
  }

  def enumerate[T](implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    enumerateJson(ec).map { e =>
      e &> Enumeratee.map[JsObject](r.reads(_)) &> Enumeratee.collect[JsResult[T]] { case JsSuccess(value, _) => value }
    }
  }

  def asJsonEnumerator(implicit ec: ExecutionContext): Enumerator[JsObject] = {
    Concurrent.unicast[JsObject](onStart = c => enumerateJson(ec).map(_(Iteratee.foreach[JsObject](c.push).map(_ => c.eofAndEnd()))))
  }

  def asEnumerator[T](implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Concurrent.unicast[T](onStart = c => enumerate[T](r, ec).map(_(Iteratee.foreach[T](c.push).map(_ => c.eofAndEnd()))))
  }

  def toJsArray(implicit ec: ExecutionContext): Future[JsArray] = {
    val result = Promise[JsValue]()
    CouchbaseN1QL.client.preparePost(url).addQueryParameter("q", query).execute(new AsyncCompletionHandler[Response]() {
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

  def toList[T](implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    enumerate[T](r, ec).flatMap(_(Iteratee.getChunks[T]).flatMap(_.run))
  }

  def headOption[T](implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    enumerate[T](r, ec).flatMap(_(Iteratee.head[T]).flatMap(_.run))
  }
}

object CouchbaseN1QL {

  private val config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
    .setAllowPoolingConnection(true)
    .setCompressionEnabled(true)
    .setRequestTimeoutInMs(600000)
    .setIdleConnectionInPoolTimeoutInMs(600000)
    .setIdleConnectionTimeoutInMs(600000)
    .build()

  private[reactivecouchbase] val client: AsyncHttpClient = new AsyncHttpClient(config)

  def N1QL(query: String, host: String, port: String, driver: ReactiveCouchbaseDriver): N1QLQuery = {
    new N1QLQuery(query, host, port)
  }

  def N1QL(query: String, driver: ReactiveCouchbaseDriver): N1QLQuery = {
    val host = driver.configuration.getString("couchbase.n1ql.host").getOrElse(throw new ReactiveCouchbaseException("Cannot find N1QL host", "Cannot find N1QL host in couchbase.n1ql conf."))
    val port = driver.configuration.getString("couchbase.n1ql.port").getOrElse(throw new ReactiveCouchbaseException("Cannot find N1QL port", "Cannot find N1QL port in couchbase.n1ql conf."))
    new N1QLQuery(query, host, port)
  }
}

