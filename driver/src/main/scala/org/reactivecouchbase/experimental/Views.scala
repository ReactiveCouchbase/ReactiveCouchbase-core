package org.reactivecouchbase.experimental

import com.ning.http.client.{Response, AsyncCompletionHandler, AsyncHttpClient, AsyncHttpClientConfig}
import com.couchbase.client.protocol.views.{Query, View}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Promise, Future, ExecutionContext}
import play.api.libs.json._
import play.api.libs.iteratee.{Enumeratee, Enumerator}
import org.reactivecouchbase.client.{RawRow, QueryEnumerator}

case class Meta(id: String, rev: String, expiration: Long, flags: Long)

case class ViewRow(document: Option[JsValue], id: Option[String], key: String, value: String, meta: Meta) {
  def toTuple = (document, id, key, value, meta)
  def withReads[A](r: Reads[A]): TypedViewRow[A] = TypedViewRow[A](document, id, key, value, meta, r)
}

case class TypedViewRow[T](document: Option[JsValue], id: Option[String], key: String, value: String, meta: Meta, reader: Reads[T]) {
  def JsResult: JsResult[T] = document.map( doc => reader.reads(doc)).getOrElse(JsError())  // TODO : cache it
  def Instance: Option[T] = JsResult match {  // TODO : cache it
    case JsSuccess(doc, _) => Some(doc)
    case JsError(errors) => None
  }
  def toTuple = (document, id, key, value, meta)
  def withReads[A](r: Reads[A]): TypedViewRow[A] = TypedViewRow[A](document, id, key, value, meta, r)
}

// TODO : no stateful object here, provide client
object Views {

  private[reactivecouchbase] val config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
    .setAllowPoolingConnection(true)
    .setCompressionEnabled(true)
    .setRequestTimeoutInMs(600000)
    .setIdleConnectionInPoolTimeoutInMs(600000)
    .setIdleConnectionTimeoutInMs(600000)
    .build()

  private[reactivecouchbase] val client: AsyncHttpClient = new AsyncHttpClient(config)

  private[experimental] def internalViewQuery(view: View, query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[JsArray] = {
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

  def rawQuery(docName: String, viewName: String, q: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Enumerator[ViewRow]] = {
    bucket.view(docName, viewName).flatMap { view => rawQuery(view, q)(bucket, ec) }
  }

  def query[T](docName: String, viewName: String, q: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[TypedViewRow[T]]] = {
    bucket.view(docName, viewName).flatMap { view => query(view, q)(bucket, r, ec) }
  }

  def query[T](view: View, q: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[TypedViewRow[T]]] = {
    internalQuery(view, q, { (doc, id, key, value, meta) => TypedViewRow[T](doc, id, key, value, meta, r) }, bucket, ec)
  }

  def rawQuery(view: View, q: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Enumerator[ViewRow]] = {
    internalQuery(view, q, { (doc, id, key, value, meta) => ViewRow(doc, id, key, value, meta) }, bucket, ec)
  }

  private[experimental] def internalQuery[R](view: View, q: Query,
        transformation: (Option[JsValue], Option[String], String, String, Meta) => R,
        bucket: CouchbaseBucket, ec: ExecutionContext): Future[Enumerator[R]] = {
    internalViewQuery(view, q)(bucket, ec).map { array =>
      Enumerator.enumerate(array.value)(ec).through(Enumeratee.map[JsValue] { slug =>
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
        transformation(doc, id, key, value, meta)
      }(ec))
    }(ec)
  }

  private[reactivecouchbase] def internalCompatRawSearch(view: View, query: Query, bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    QueryEnumerator(internalViewQuery(view, query)(bucket, ec).map { array =>
      Enumerator.enumerate(array.value)(ec).through(Enumeratee.map[JsValue] { slug =>
        val key = (slug \ "key").asOpt[String].getOrElse("")
        val value = (slug \ "value").asOpt[String].getOrElse("")
        val id = (slug \ "id").asOpt[String]
        val doc = (slug \ "doc" \ "json").asOpt[JsValue].map(Json.stringify)
        RawRow(doc, id, key, value)
      }(ec))
    }(ec))
  }
}
