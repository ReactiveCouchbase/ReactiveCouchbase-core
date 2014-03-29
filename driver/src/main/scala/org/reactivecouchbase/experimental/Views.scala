package org.reactivecouchbase.experimental

import com.ning.http.client.{Response, AsyncCompletionHandler}
import com.couchbase.client.protocol.views.{Query, View}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Promise, Future, ExecutionContext}
import play.api.libs.json._
import play.api.libs.iteratee.{Enumeratee, Enumerator}
import org.reactivecouchbase.client.{ReactiveCouchbaseException, RawRow, QueryEnumerator}
import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import akka.pattern._
import java.util.concurrent.atomic.AtomicReference
import akka.routing.RoundRobinRouter
import akka.util.Timeout
import java.util.concurrent.TimeUnit

/**
 *
 * Metadata for each retrieved document
 *
 * @param id the key of the document
 * @param rev the revision of the document
 * @param expiration the expiration (UNIX epoch ???)
 * @param flags flags of the document
 */
case class Meta(id: String, rev: String, expiration: Long, flags: Long)

/**
 *
 * Representation of a document from a Query (with raw values)
 *
 * @param document maybe contains the value of the stored document
 * @param id maybe the key of the document (can be None if reduced query)
 * @param key the index key of the document
 * @param value the value
 * @param meta the metadata of the doc
 */
case class ViewRow(document: Option[JsValue], id: Option[String], key: String, value: String, meta: Meta) {
  def toTuple = (document, id, key, value, meta)
  def withReads[A](r: Reads[A]): TypedViewRow[A] = TypedViewRow[A](document, id, key, value, meta, r)
}

/**
 *
 * Representation of a document from a Query (with typed values)
 *
 * @param document maybe contains the value of the stored document
 * @param id maybe the key of the document (can be None if reduced query)
 * @param key the index key of the document
 * @param value the value
 * @param meta the metadata of the doc
 * @param reader Json reader for the document
 * @tparam T the type of the document
 */
case class TypedViewRow[T](document: Option[JsValue], id: Option[String], key: String, value: String, meta: Meta, reader: Reads[T]) {
  def JsResult: JsResult[T] = document.map( doc => reader.reads(doc)).getOrElse(JsError())  // TODO : cache it
  def Instance: Option[T] = JsResult match {  // TODO : cache it
    case JsSuccess(doc, _) => Some(doc)
    case JsError(errors) => None
  }
  def toTuple = (document, id, key, value, meta)
  def withReads[A](r: Reads[A]): TypedViewRow[A] = TypedViewRow[A](document, id, key, value, meta, r)
}


private[experimental] case class SendQuery(view: View, query: Query, bucket: CouchbaseBucket, ec: ExecutionContext)
private[experimental] case class QueryResponse(response: JsArray)
private[experimental] class QueryWorker extends Actor with ActorLogging {
  def receive = {
    case SendQuery(view, query, bucket, ec) => {
      val from = sender
      val name = self.path.name
      bucket.logger.debug(s"Query from actor : $name")
      Views.__internalViewQuery(view, query)(bucket, ec).map { array => from ! array }(ec)
    }
    case _ =>
  }
}

/**
 * Custom API for Couchbase view querying
 */
object Views {

  private[experimental] val workers: AtomicReference[Option[ActorRef]] = new AtomicReference(None)

  private[experimental] def pass(view: View, query: Query, bucket: CouchbaseBucket, ec: ExecutionContext): Future[JsArray] = {
    implicit val timeout = Timeout(bucket.ecTimeout, TimeUnit.MILLISECONDS)
    val ref: ActorRef = workers.get.getOrElse({
       workers.set(Some(bucket.cbDriver.system().actorOf(Props[QueryWorker].withRouter(RoundRobinRouter(bucket.workersNbr)), "queries-dispatcher")))
       workers.get().get
    })
    (ref ? SendQuery(view, query, bucket, ec)).mapTo[JsArray]
  }

  /**
   *
   * Perfom the actual HTTP request to query views using ning async http client
   *
   * @param view the view used for querying
   * @param query the actual query
   * @param bucket the bucket used for querying
   * @param ec the ExecutionContext used for async processing
   * @return the future JsArray result
   */
  private[experimental] def __internalViewQuery(view: View, query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[JsArray] = {
    val url = s"http://${bucket.hosts.head}:8092${view.getURI}${query.toString}&include_docs=${query.willIncludeDocs()}"
    val promise = Promise[String]()
    bucket.httpClient.prepareGet(url).execute(new AsyncCompletionHandler[Response]() {
      override def onCompleted(response: Response) = {
        if (response.getStatusCode != 200) {
          promise.failure(new ReactiveCouchbaseException("Error", s"Couchbase responded with status '${response.getStatusCode}' : ${response.getResponseBody}"))
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

  /**
   *
   * Raw query without providing an actual view
   *
   * @param docName name of the design doc
   * @param viewName name of the view
   * @param q the actual query
   * @param bucket the bucket used for querying
   * @param ec the ExecutionContext used for async processing
   * @return the future enumerator of documents
   */
  def rawQuery(docName: String, viewName: String, q: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Enumerator[ViewRow]] = {
    bucket.view(docName, viewName).flatMap { view => rawQuery(view, q)(bucket, ec) }
  }

  /**
   *
   * Typed query without providing an actual view
   *
   * @param docName
   * @param viewName
   * @param q the actual query
   * @param bucket the bucket used for querying
   * @param r
   * @param ec the ExecutionContext used for async processing
   * @tparam T the type of documents
   * @return the future enumerator of documents
   */
  def query[T](docName: String, viewName: String, q: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[TypedViewRow[T]]] = {
    bucket.view(docName, viewName).flatMap { view => query(view, q)(bucket, r, ec) }
  }

  /**
   *
   * Typed query
   *
   * @param view the view used for querying
   * @param q the actual query
   * @param bucket the bucket used for querying
   * @param r
   * @param ec the ExecutionContext used for async processing
   * @tparam T the type of documents
   * @return the future enumerator of documents
   */
  def query[T](view: View, q: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[TypedViewRow[T]]] = {
    internalQuery(view, q, { (doc, id, key, value, meta) => TypedViewRow[T](doc, id, key, value, meta, r) }, bucket, ec)
  }

  /**
   *
   * Raw Query
   *
   * @param view the view used for querying
   * @param q the actual query
   * @param bucket the bucket used for querying
   * @param ec the ExecutionContext used for async processing
   * @return the future enumerator of documents
   */
  def rawQuery(view: View, q: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Enumerator[ViewRow]] = {
    internalQuery(view, q, { (doc, id, key, value, meta) => ViewRow(doc, id, key, value, meta) }, bucket, ec)
  }

  /**
   *
   * Internal method that transform JsArray to an Enumerator of R
   *
   * @param view the view for querying
   * @param q the actual query
   * @param transformation a function to transform slug data to R
   * @param bucket the bucket used for querying
   * @param ec the ExecutionContext used for async processing
   * @tparam R the type of slugs (JsValue for something with Json format)
   * @return the future Enumerator of R
   */
  private[experimental] def internalQuery[R](view: View, q: Query,
        transformation: (Option[JsValue], Option[String], String, String, Meta) => R,
        bucket: CouchbaseBucket, ec: ExecutionContext): Future[Enumerator[R]] = {
    pass(view, q, bucket, ec).map { array =>
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

  /**
   *
   * Bridge to use the experimental query API from the official API if flag is on
   *
   * @param view the view for querying
   * @param query the actual query
   * @param bucket the bucket used for querying
   * @param ec the ExecutionContext used for async processing
   * @return the QueryEnumerator that can stream the result of the query
   */
  private[reactivecouchbase] def internalCompatRawSearch(view: View, query: Query, bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    QueryEnumerator(() => pass(view, query, bucket, ec).map { array =>
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
