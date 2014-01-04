package org.reactivecouchbase.experimental

import com.ning.http.client.{Response, AsyncCompletionHandler, AsyncHttpClient, AsyncHttpClientConfig}
import com.couchbase.client.protocol.views.{Query, View}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Promise, Future, ExecutionContext}
import play.api.libs.json._
import play.api.libs.iteratee.{Enumeratee, Enumerator}

case class Meta(id: String, rev: String, expiration: Long, flags: Long)

case class ViewRow(document: Option[JsValue], id: Option[String], key: String, value: String, meta: Meta) {
  def toTuple = (document, id, key, value, meta)
}

object Views {

  private val config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
    .setAllowPoolingConnection(true)
    .setCompressionEnabled(true)
    .setRequestTimeoutInMs(600000)
    .setIdleConnectionInPoolTimeoutInMs(600000)
    .setIdleConnectionTimeoutInMs(600000)
    .build()

  private[reactivecouchbase] val client: AsyncHttpClient = new AsyncHttpClient(config)

  private[experimental] def viewQuery(view: View, query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[JsArray] = {
    val url = s"http://${bucket.hosts.head}:8092${view.getURI}${query.toString}&include_docs=${query.willIncludeDocs()}"
    val promise = Promise[String]()
    client.prepareGet(url).execute(new AsyncCompletionHandler[Response]() {
      override def onCompleted(response: Response) = {
        if (response.getStatusCode != 200) {
          promise.failure(new RuntimeException(s"Couchbase responded with status '${response.getStatusCode}' : ${response.getResponseBody}"))
        } else {
          promise.success(response.getResponseBody)
        }
        response
      }
      override def onThrowable(t: Throwable) = {
        promise.failure(t)
      }
    })
    promise.future.map(body => (Json.parse(body) \ "rows").as[JsArray])
  }

  def query(docName: String, viewName: String, q: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Enumerator[ViewRow]] = {
    bucket.view(docName, viewName).flatMap { view => query(view, q)(bucket, ec) }
  }

  def query(view: View, q: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Enumerator[ViewRow]] = {
    viewQuery(view, q)(bucket, ec).map { array =>
      Enumerator.enumerate(array.value).through(Enumeratee.map { slug =>
        val meta = Meta(
          (slug \ "doc" \ "meta" \ "id").asOpt[String].getOrElse(""),
          (slug \ "doc" \ "meta" \ "rev").asOpt[String].getOrElse(""),
          (slug \ "doc" \ "meta" \ "expiration").asOpt[Long].getOrElse(0L),
          (slug \ "doc" \ "meta" \ "flags").asOpt[Long].getOrElse(0L)
        )
        val key = (slug \ "key").asOpt[String].getOrElse("")
        val value = (slug \ "value").asOpt[String].getOrElse("")
        val id = (slug \ "id").asOpt[String]
        val doc = (slug \ "doc" \ "json").asOpt[JsValue]
        //if (q.willReduce())   // no doc and id
        ViewRow(doc, id, key, value, meta)
      })
    }
  }
}
