package org.reactivecouchbase.client

import play.api.libs.json._
import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.iteratee.{Enumeratee, Iteratee, Concurrent, Enumerator}
import com.couchbase.client.protocol.views._
import org.reactivecouchbase.{Couchbase, CouchbaseBucket}
import java.util.concurrent.TimeUnit
import org.reactivecouchbase.client.CouchbaseFutures._
import net.spy.memcached.ops.OperationStatus
import collection.JavaConversions._
import play.api.libs.json.JsSuccess
import scala.Some
import play.api.libs.json.JsObject

case class RawRow(document: Option[String], id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

case class JsRow[T](document: JsResult[T], id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

case class TypedRow[T](document: T, id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

class QueryEnumerator[T](futureEnumerator: Future[Enumerator[T]]) {
  def enumerate: Future[Enumerator[T]] = futureEnumerator
  def enumerated(implicit ec: ExecutionContext): Enumerator[T] =
    Concurrent.unicast[T](onStart = c => futureEnumerator.map(_(Iteratee.foreach[T](c.push).map(_ => c.eofAndEnd()))))

  def toList(implicit ec: ExecutionContext): Future[List[T]] =
    futureEnumerator.flatMap(_(Iteratee.getChunks[T]).flatMap(_.run))

  def headOption(implicit ec: ExecutionContext): Future[Option[T]] =
    futureEnumerator.flatMap(_(Iteratee.head[T]).flatMap(_.run))
}

object QueryEnumerator {
  def apply[T](enumerate: Future[Enumerator[T]]): QueryEnumerator[T] = new QueryEnumerator[T](enumerate)
}

trait Queries {

  def docName(name: String) = {
    if (play.api.Play.isDev(play.api.Play.current)) s"dev_$name" else name
  }

  def find[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(docName, viewName)(query)(bucket, r, ec).toList(ec)
  def find[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(view)(query)(bucket, r, ec).toList(ec)

  ///////////////////////////////////////////////////////////////////////////////////////////////////

  def rawSearch(docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => rawSearch(view)(query)(bucket, ec).enumerate
      case _ => Future.failed(new PlayException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  def rawSearch(view: View)(query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    QueryEnumerator(waitForHttp[ViewResponse]( bucket.couchbaseClient.asyncQuery(view, query), ec ).map { results =>
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

  def search[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => search(view)(query)(bucket, r, ec).enumerate
      case _ => Future.failed(new PlayException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

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
          case JsRow(JsError(errors), _, _, _) if Constants.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(errors))
        }
    })
  }

  def searchValues[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => searchValues(view)(query)(bucket, r, ec).enumerate
      case _ => Future.failed(new PlayException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  def searchValues[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(search[T](view)(query)(bucket, r, ec).enumerate.map { enumerator =>
      enumerator &> Enumeratee.map[TypedRow[T]](_.document)
    })
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * The view must index numbers
   */
  def tailableQuery[T](doc: String, view: String, extractor: T => Long, from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    var last = from
    def query() = {
      new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false).setRangeStart(ComplexKey.of(last.asInstanceOf[AnyRef])).setRangeEnd(ComplexKey.of(Long.MaxValue.asInstanceOf[AnyRef]))
    }
    Enumerator.repeatM({
      val actualQuery = query()
      play.api.libs.concurrent.Promise.timeout(Some, every, unit).flatMap(_ => Couchbase.find[T](doc, view)(actualQuery)(bucket, r, ec))
    }).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( _ => true ) ).through(Enumeratee.map { elem =>
      last = extractor(elem) + 1L
      elem
    })
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    pollQuery[T](doc, view, query, everyMillis, { chunk: T => true })(bucket, r, ec)
  }

  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long, filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      play.api.libs.concurrent.Promise.timeout(Some, everyMillis, TimeUnit.MILLISECONDS).flatMap(_ => find[T](doc, view)(query)(bucket, r, ec))
    ).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( filter ) )
  }

  def repeatQuery[T](doc: String, view: String, query: Query, trigger: Future[AnyRef])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, trigger, { chunk: T => true })(bucket, r, ec)
  }

  def repeatQuery[T](doc: String, view: String, query: Query, trigger: Future[AnyRef], filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      trigger.flatMap { _ => find[T](doc, view)(query)(bucket, r, ec) }
    ).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( filter ) )
  }

  def repeatQuery[T](doc: String, view: String, query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, { chunk: T => true })(bucket, r, ec)
  }

  def repeatQuery[T](doc: String, view: String, query: Query, filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, Future.successful(Some),filter)(bucket, r, ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def view(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    waitForHttp[View]( bucket.couchbaseClient.asyncGetView(docName, viewName), ec )
  }

  def spatialView(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[SpatialView] = {
    waitForHttp[SpatialView]( bucket.couchbaseClient.asyncGetSpatialView(docName, viewName), ec )
  }

  def designDocument(docName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[DesignDocument] = {
    waitForHttp[DesignDocument]( bucket.couchbaseClient.asyncGetDesignDocument(docName), ec )
  }

  def createDesignDoc(name: String, value: JsObject)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(name, Json.stringify(value)), ec )
  }

  def createDesignDoc(name: String, value: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(name, value), ec )
  }

  def createDesignDoc(value: DesignDocument)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncCreateDesignDoc(value), ec )
  }

  def deleteDesignDoc(name: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForHttpStatus( bucket.couchbaseClient.asyncDeleteDesignDoc(name), ec )
  }
}