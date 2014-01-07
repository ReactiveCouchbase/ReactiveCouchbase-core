package org.reactivecouchbase.client

import play.api.libs.json._
import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.iteratee.{Enumeratee, Iteratee, Concurrent, Enumerator}
import com.couchbase.client.protocol.views._
import org.reactivecouchbase.{Timeout, Couchbase, CouchbaseBucket}
import java.util.concurrent.TimeUnit
import org.reactivecouchbase.client.CouchbaseFutures._
import net.spy.memcached.ops.OperationStatus
import collection.JavaConversions._
import play.api.libs.json.JsSuccess
import scala.Some
import play.api.libs.json.JsObject
import org.reactivecouchbase.experimental.Views

/**
 *
 * Raw row query result
 *
 * @param document the document
 * @param id the documents key
 * @param key the documents indexed key
 * @param value the documents indexed value
 */
case class RawRow(document: Option[String], id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

/**
 *
 * Js row query result
 *
 * @param document the document
 * @param id the documents key
 * @param key the documents indexed key
 * @param value the documents indexed value
 * @tparam T the type of the doc
 */
case class JsRow[T](document: JsResult[T], id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

/**
 *
 * Typed row query result
 *
 * @param document the document
 * @param id the documents key
 * @param key the documents indexed key
 * @param value the documents indexed value
 * @tparam T the type of the doc
 */
case class TypedRow[T](document: T, id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

/**
 *
 * Reactive representation of a query result
 *
 * @param futureEnumerator doc stream
 * @tparam T type of doc
 */
class QueryEnumerator[T](futureEnumerator: Future[Enumerator[T]]) {

  /**
   * @return the enumerator for query results
   */
  def enumerate: Future[Enumerator[T]] = futureEnumerator
  def enumerated(implicit ec: ExecutionContext): Enumerator[T] =
    Concurrent.unicast[T](onStart = c => futureEnumerator.map(_(Iteratee.foreach[T](c.push).map(_ => c.eofAndEnd()))))

  /**
   *
   * @param ec
   * @return
   */
  def toList(implicit ec: ExecutionContext): Future[List[T]] =
    futureEnumerator.flatMap(_(Iteratee.getChunks[T]).flatMap(_.run))

  def headOption(implicit ec: ExecutionContext): Future[Option[T]] =
    futureEnumerator.flatMap(_(Iteratee.head[T]).flatMap(_.run))
}

/**
 * Companion object to build QueryEnumerators
 */
object QueryEnumerator {
  def apply[T](enumerate: Future[Enumerator[T]]): QueryEnumerator[T] = new QueryEnumerator[T](enumerate)
}

/**
 * Trait to query Couchbase
 */
trait Queries {

  def docName(name: String) = {
    name//if (play.api.Play.isDev(play.api.Play.current)) s"dev_$name" else name
  }

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param query
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def find[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(docName, viewName)(query)(bucket, r, ec).toList(ec)

  /**
   *
   *
   *
   * @param view
   * @param query
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def find[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(view)(query)(bucket, r, ec).toList(ec)

  ///////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param query
   * @param bucket
   * @param ec
   * @return
   */
  def rawSearch(docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => rawSearch(view)(query)(bucket, ec).enumerate
      case _ => Future.failed(new ReactiveCouchbaseException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  /**
   *
   *
   *
   * @param view
   * @param query
   * @param bucket
   * @param ec
   * @return
   */
  def rawSearch(view: View)(query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    if (bucket.useExperimentalQueries) {
      Views.internalCompatRawSearch(view, query, bucket, ec)
    } else {
      QueryEnumerator(waitForHttp[ViewResponse]( bucket.couchbaseClient.asyncQuery(view, query), bucket, ec ).map { results =>
        Enumerator.enumerate(results.iterator()) &> Enumeratee.map[ViewRow] {
          case r: ViewRowWithDocs if query.willIncludeDocs() => RawRow(Some(r.getDocument.asInstanceOf[String]), Some(r.getId), r.getKey, r.getValue)
          case r: ViewRowWithDocs if !query.willIncludeDocs() => RawRow(None, Some(r.getId), r.getKey, r.getValue)
          case r: ViewRowNoDocs => RawRow(None, Some(r.getId), r.getKey, r.getValue)
          case r: ViewRowReduced => RawRow(None, None, r.getKey, r.getValue)
          case r: SpatialViewRowNoDocs => RawRow(None, Some(r.getId), r.getKey, r.getValue)
          case r: SpatialViewRowWithDocs if query.willIncludeDocs() => RawRow(Some(r.getDocument.asInstanceOf[String]), Some(r.getId), r.getKey, r.getValue)
          case r: SpatialViewRowWithDocs if !query.willIncludeDocs() => RawRow(None, Some(r.getId), r.getKey, r.getValue)
        }
      })
    }
  }

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param query
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def search[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => search(view)(query)(bucket, r, ec).enumerate
      case _ => Future.failed(new ReactiveCouchbaseException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  /**
   *
   *
   *
   * @param view
   * @param query
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def search[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = {
    QueryEnumerator(rawSearch(view)(query)(bucket, ec).enumerate.map { enumerator =>
      enumerator &>
        Enumeratee.map[RawRow] { row =>
          row.document.map { doc =>
            JsRow[T](r.reads(Json.parse(doc)), row.id, row.key, row.value)
          }.getOrElse(
            JsRow[T](JsError(), row.id, row.key, row.value)
          )
        } &>
        Enumeratee.collect[JsRow[T]] {
          case JsRow(JsSuccess(doc, _), id, key, value) => TypedRow[T](doc, id, key, value)
          case JsRow(JsError(errors), _, _, _) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(errors))
        }
    })
  }

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param query
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def searchValues[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => searchValues(view)(query)(bucket, r, ec).enumerate
      case _ => Future.failed(new ReactiveCouchbaseException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  /**
   *
   *
   *
   * @param view
   * @param query
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def searchValues[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(search[T](view)(query)(bucket, r, ec).enumerate.map { enumerator =>
      enumerator &> Enumeratee.map[TypedRow[T]](_.document)
    })
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param extractor
   * @param from
   * @param every
   * @param unit
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def tailableQuery[T](doc: String, view: String, extractor: T => Long, from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    var last = from
    def query() = {
      new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false).setRangeStart(ComplexKey.of(last.asInstanceOf[AnyRef])).setRangeEnd(ComplexKey.of(Long.MaxValue.asInstanceOf[AnyRef]))
    }
    Enumerator.repeatM({
      val actualQuery = query()
      Timeout.timeout(Some, every, unit, bucket.driver.scheduler()).flatMap(_ => Couchbase.find[T](doc, view)(actualQuery)(bucket, r, ec))
    }).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( _ => true ) ).through(Enumeratee.map { elem =>
      last = extractor(elem) + 1L
      elem
    })
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param query
   * @param everyMillis
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    pollQuery[T](doc, view, query, everyMillis, { chunk: T => true })(bucket, r, ec)
  }

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param query
   * @param everyMillis
   * @param filter
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long, filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      Timeout.timeout(Some, everyMillis, TimeUnit.MILLISECONDS, bucket.driver.scheduler()).flatMap(_ => find[T](doc, view)(query)(bucket, r, ec))
    ).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( filter ) )
  }

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param query
   * @param trigger
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def repeatQuery[T](doc: String, view: String, query: Query, trigger: Future[AnyRef])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, trigger, { chunk: T => true })(bucket, r, ec)
  }

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param query
   * @param trigger
   * @param filter
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def repeatQuery[T](doc: String, view: String, query: Query, trigger: Future[AnyRef], filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      trigger.flatMap { _ => find[T](doc, view)(query)(bucket, r, ec) }
    ).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( filter ) )
  }

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param query
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def repeatQuery[T](doc: String, view: String, query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, { chunk: T => true })(bucket, r, ec)
  }

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param query
   * @param filter
   * @param bucket
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def repeatQuery[T](doc: String, view: String, query: Query, filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, Future.successful(Some),filter)(bucket, r, ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param bucket
   * @param ec
   * @return
   */
  def view(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    waitForHttp[View]( bucket.couchbaseClient.asyncGetView(docName, viewName), bucket, ec )
  }

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param bucket
   * @param ec
   * @return
   */
  def spatialView(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[SpatialView] = {
    waitForHttp[SpatialView]( bucket.couchbaseClient.asyncGetSpatialView(docName, viewName), bucket, ec )
  }

  /**
   *
   *
   *
   * @param docName
   * @param bucket
   * @param ec
   * @return
   */
  def designDocument(docName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[DesignDocument] = {
    waitForHttp[DesignDocument]( bucket.couchbaseClient.asyncGetDesignDocument(docName), bucket, ec )
  }

  /**
   *
   *
   *
   * @param name
   * @param value
   * @param bucket
   * @param ec
   * @return
   */
  def createDesignDoc(name: String, value: JsObject)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(name, Json.stringify(value)), bucket, ec )
  }

  /**
   *
   *
   *
   * @param name
   * @param value
   * @param bucket
   * @param ec
   * @return
   */
  def createDesignDoc(name: String, value: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(name, value), bucket, ec )
  }

  /**
   *
   *
   *
   * @param value
   * @param bucket
   * @param ec
   * @return
   */
  def createDesignDoc(value: DesignDocument)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(value), bucket, ec )
  }

  /**
   *
   *
   *
   * @param name
   * @param bucket
   * @param ec
   * @return
   */
  def deleteDesignDoc(name: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncDeleteDesignDoc(name), bucket, ec )
  }
}