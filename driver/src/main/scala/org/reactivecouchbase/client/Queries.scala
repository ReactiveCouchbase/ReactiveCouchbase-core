package org.reactivecouchbase.client

import play.api.libs.json._
import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import com.couchbase.client.protocol.views._
import org.reactivecouchbase.{Timeout, Couchbase, CouchbaseBucket}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import org.reactivecouchbase.client.CouchbaseFutures._
import collection.JavaConversions._
import org.reactivecouchbase.experimental.Views
import play.api.libs.json.JsSuccess
import scala.Some
import play.api.libs.json.JsObject

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
 * @tparam T type of the doc the type of the doc
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
 * @tparam T type of the doc the type of the doc
 */
case class TypedRow[T](document: T, id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

/**
 *
 * ReactiveCouchbase representation of a query result
 *
 * @param futureEnumerator doc stream
 * @tparam T type of the doc type of doc
 */
class QueryEnumerator[T](futureEnumerator: () => Future[Enumerator[T]]) {

  /**
   * @return the enumerator for query results
   */
  def toEnumerator: Future[Enumerator[T]] = futureEnumerator()

  /**
   * 
   * @param ec ExecutionContext for async processing
   * @return the query result as enumerator
   */
  def enumerate(implicit ec: ExecutionContext): Enumerator[T] =
    Enumerator.flatten(futureEnumerator())
    //Concurrent.unicast[T](onStart = c => futureEnumerator.map(_(Iteratee.foreach[T](c.push).map(_ => c.eofAndEnd()))))

  /**
   *
   * @param ec ExecutionContext for async processing
   * @return the query result as list
   */
  def toList(implicit ec: ExecutionContext): Future[List[T]] =
    futureEnumerator().flatMap(_(Iteratee.getChunks[T]).flatMap(_.run))

  /**
   * 
   * @param ec ExecutionContext for async processing
   * @return the optinal head
   */
  def headOption(implicit ec: ExecutionContext): Future[Option[T]] =
    futureEnumerator().flatMap(_(Iteratee.head[T]).flatMap(_.run))
}

/**
 * Companion object to build QueryEnumerators
 */
object QueryEnumerator {
  def apply[T](enumerate: () => Future[Enumerator[T]]): QueryEnumerator[T] = new QueryEnumerator[T](enumerate)
}

/**
 * Trait to query Couchbase
 */
trait Queries {

  def docName(name: String)(implicit bucket: CouchbaseBucket) = {
    s"${bucket.cbDriver.mode}${name}"
  }

  /**
   *
   * Perform a Couchbase query on a view
   *
   * @param docName the name of the design doc
   * @param viewName the name of the view
   * @param query the actual query
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the list of docs
   */
  def find[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(docName, viewName)(query)(bucket, r, ec).toList(ec)

  /**
   *
   * Perform a Couchbase query on a view
   *
   * @param view the view to query
   * @param query the actual query
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the list of docs
   */
  def find[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(view)(query)(bucket, r, ec).toList(ec)

  ///////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   *
   * Perform a Couchbase query on a view
   *
   * @param docName the name of the design doc
   * @param viewName the name of the view
   * @param query the actual query
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the query enumerator
   */
  def rawSearch(docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    QueryEnumerator(() => view(docName, viewName).flatMap {
      case view: View => rawSearch(view)(query)(bucket, ec).toEnumerator
      case _ => Future.failed(new ReactiveCouchbaseException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  /**
   *
   * Perform a Couchbase query on a view
   *
   * @param view the view to query
   * @param query the actual query
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the query enumerator
   */
  def rawSearch(view: View)(query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    if (bucket.useExperimentalQueries) {
      Views.internalCompatRawSearch(view, query, bucket, ec)
    } else {
      QueryEnumerator(() => waitForHttp[ViewResponse]( bucket.couchbaseClient.asyncQuery(view, query), bucket, ec ).map { results =>
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
   * Perform a Couchbase query on a view
   *
   * @param docName the name of the design doc
   * @param viewName the name of the view
   * @param query the actual query
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def search[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = {
    QueryEnumerator(() => view(docName, viewName).flatMap {
      case view: View => search(view)(query)(bucket, r, ec).toEnumerator
      case _ => Future.failed(new ReactiveCouchbaseException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  /**
   *
   * Perform a Couchbase query on a view
   *
   * @param view the view to query
   * @param query the actual query
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def search[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = {
    QueryEnumerator(() => rawSearch(view)(query)(bucket, ec).toEnumerator.map { enumerator =>
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
   * Perform a Couchbase query on a view
   *
   * @param docName the name of the design doc
   * @param viewName the name of the view
   * @param query the actual query
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def searchValues[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(() => view(docName, viewName).flatMap {
      case view: View => searchValues(view)(query)(bucket, r, ec).toEnumerator
      case _ => Future.failed(new ReactiveCouchbaseException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  /**
   *
   * Perform a Couchbase query on a view
   *
   * @param view the view to query
   * @param query the actual query
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def searchValues[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(() => search[T](view)(query)(bucket, r, ec).toEnumerator.map { enumerator =>
      enumerator &> Enumeratee.map[TypedRow[T]](_.document)
    })
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   * 'Tail -f' on a query
   *
   * @param doc the name of the design doc
   * @param view the view to query
   * @param extractor id extrator of natural insertion order
   * @param from start from id
   * @param every tail every
   * @param unit unit of time
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def tailableQuery[T](doc: String, view: String, extractor: T => Long, from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit bucket: () => CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    var last = System.currentTimeMillis()
    def query() = new Query()
      .setIncludeDocs(true)
      .setStale(Stale.FALSE)
      .setDescending(false)
      .setRangeStart(ComplexKey.of(last.asInstanceOf[AnyRef]))
      .setRangeEnd(ComplexKey.of(Long.MaxValue.asInstanceOf[AnyRef]))

    def step(list: ConcurrentLinkedQueue[T]): Future[Option[(ConcurrentLinkedQueue[T], T)]] = {
      Couchbase.find[T](doc, view)(query())(bucket(), r, ec).map { res =>
        res.foreach { doc =>
          last = extractor(doc) + 1L
          list.offer(doc)
        }
      }.flatMap { _ =>
        list.poll() match {
          case null => Timeout.timeout("", every, unit, bucket().driver.scheduler()).flatMap(_ => step(list))
          case e => Future.successful(Some((list, e)))
        }
      }
    }
    Enumerator.unfoldM(new ConcurrentLinkedQueue[T]()) { list =>
      if (list.isEmpty) {
        step(list)
      } else {
        val el = list.poll()
        Future.successful(Some((list, el)))
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   * Poll query every n millisec
   *
   * @param doc the name of the design doc
   * @param view the view to query
   * @param query the actual query
   * @param everyMillis repeat every ...
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    pollQuery[T](doc, view, query, everyMillis, { chunk: T => true })(bucket, r, ec)
  }

  /**
   *
   * Poll query every n millisec
   *
   * @param doc the name of the design doc
   * @param view the view to query
   * @param query the actual query
   * @param everyMillis repeat every ...
   * @param filter the filter for documents selection
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long, filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      Timeout.timeout(Some, everyMillis, TimeUnit.MILLISECONDS, bucket.driver.scheduler()).flatMap(_ => find[T](doc, view)(query)(bucket, r, ec))
    ).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( filter ) )
  }

  /**
   *
   * Repeat a query each time trigger is done
   *
   * @param doc the name of the design doc
   * @param view the view to query
   * @param query the actual query
   * @param trigger trigger the repeat when future is done
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def repeatQuery[T](doc: String, view: String, query: Query, trigger: Future[AnyRef])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, trigger, { chunk: T => true })(bucket, r, ec)
  }

  /**
   *
   * Repeat a query each time trigger is done
   *
   * @param doc the name of the design doc
   * @param view the view to query
   * @param query the actual query
   * @param trigger trigger the repeat when future is done
   * @param filter the filter for documents selection
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def repeatQuery[T](doc: String, view: String, query: Query, trigger: Future[AnyRef], filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      trigger.flatMap { _ => find[T](doc, view)(query)(bucket, r, ec) }
    ).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( filter ) )
  }

  /**
   *
   * Repeat a query indefinitely
   *
   * @param doc the name of the design doc
   * @param view  the view to query
   * @param query the actual query
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def repeatQuery[T](doc: String, view: String, query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, { chunk: T => true })(bucket, r, ec)
  }

  /**
   *
   * Repeat a query indefinitely
   *
   * @param doc the name of the design doc
   * @param view the view to query
   * @param query the actual query
   * @param filter the filter for documents selection
   * @param bucket the bucket to use
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the query enumerator
   */
  def repeatQuery[T](doc: String, view: String, query: Query, filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, Future.successful(Some),filter)(bucket, r, ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   *
   * Fetch a view
   *
   * @param docName the name of the design doc
   * @param viewName the name of the view
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the view
   */
  def view(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    waitForHttp[View]( bucket.couchbaseClient.asyncGetView(docName, viewName), bucket, ec )
  }

  /**
   *
   * Fetch a spatial view
   *
   * @param docName the name of the design doc
   * @param viewName the name of the view
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the spatial view
   */
  def spatialView(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[SpatialView] = {
    waitForHttp[SpatialView]( bucket.couchbaseClient.asyncGetSpatialView(docName, viewName), bucket, ec )
  }

  /**
   *
   * Fetch a design document
   *
   * @param docName the name of the design doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return fetch design doc
   */
  def designDocument(docName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[DesignDocument] = {
    waitForHttp[DesignDocument]( bucket.couchbaseClient.asyncGetDesignDocument(docName), bucket, ec )
  }

  /**
   *
   * Create a design doc
   *
   * @param name name of the design doc
   * @param value the content of the design doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def createDesignDoc(name: String, value: JsObject)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(name, Json.stringify(value)), bucket, ec ).map(OpResult(_, 1, Some(value)))
  }

  /**
   *
   * Create a design doc
   *
   * @param name name of the design doc
   * @param value the content of the design doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def createDesignDoc(name: String, value: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(name, value), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Create a design doc
   *
   * @param value the content of the design doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def createDesignDoc(value: DesignDocument)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(value), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Delete a design doc
   *
   * @param name name of the design doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def deleteDesignDoc(name: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncDeleteDesignDoc(name), bucket, ec ).map(OpResult(_, 1))
  }
}