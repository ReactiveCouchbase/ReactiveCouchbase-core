package org.reactivecouchbase.client

import org.reactivecouchbase.{CouchbaseRWImplicits, CouchbaseBucket, Couchbase}
import com.couchbase.client.protocol.views.{Stale, Query}
import play.api.libs.json._
import scala.concurrent.{Promise, Future, ExecutionContext}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import play.api.libs.iteratee.{Enumeratee, Enumerator}
import net.spy.memcached.{ReplicateTo, PersistTo}
import net.spy.memcached.ops.OperationStatus
import scala.concurrent.duration.Duration
import org.reactivecouchbase.CouchbaseExpiration._


object CappedBucket {
  private val buckets = new ConcurrentHashMap[String, CappedBucket]()
  private def docName = "play2couchbase-cappedbucket-designdoc"
  private def viewName = "byNaturalOrder"
  private def cappedRef = "__playcbcapped"
  private def cappedNaturalId = "__playcbcappednatural"
  private def designDoc =
    s"""
      {
        "views":{
           "byNaturalOrder": {
               "map": "function (doc, meta) { if (doc.$cappedRef) { if (doc.$cappedNaturalId) { emit(doc.$cappedNaturalId, null); } } } "
           }
        }
      }
    """

  def apply(name: String, max: Int, reaper: Boolean = true)(implicit app: Application) = {
    if (!buckets.containsKey(name)) {
      buckets.putIfAbsent(name, new CappedBucket(name, max, reaper)(app))
    }
    buckets.get(name)
  }

  private def enabledReaper(bucket: CouchbaseBucket, max: Int, app: Application, ec: ExecutionContext) = {
    if (!reaperOn.containsKey(bucket.bucket)) {
      reaperOn.putIfAbsent(bucket.bucket, true)
      Logger.info(s"Capped reaper is on for ${bucket.bucket} ...")
      play.api.libs.concurrent.Akka.system(app).scheduler.schedule(Duration(0, TimeUnit.MILLISECONDS), Duration(1000, TimeUnit.MILLISECONDS))({
        val query = new Query().setIncludeDocs(false).setStale(Stale.FALSE).setDescending(true).setSkip(max)
        bucket.rawSearch(docName, viewName)(query)(ec).toList(ec).map { f =>
          f.map { elem =>
            bucket.delete(elem.id.get)(ec)
          }}(ec)
      })(ec)
    }
  }

  private lazy val reaperOn = new ConcurrentHashMap[String, Boolean]()
  private lazy val triggerPromise = Promise[Unit]()
  private lazy val trigger = triggerPromise.future

  private def setupViews(bucket: CouchbaseBucket, ec: ExecutionContext) = {
    bucket.createDesignDoc(CappedBucket.docName, CappedBucket.designDoc)(ec).map(_ => triggerPromise.success(()))(ec)
  }
}

class CappedBucket(name: String, max: Int, reaper: Boolean = true)(implicit app: Application) {

  def bucket = Couchbase.bucket(name)

  if (!CappedBucket.triggerPromise.isCompleted) CappedBucket.setupViews(bucket, Couchbase.couchbaseExecutor(app))
  if (reaper) CappedBucket.enabledReaper(bucket, max, app, Couchbase.couchbaseExecutor(app))

  def oldestOption[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false).setLimit(1)
    CappedBucket.trigger.flatMap(_ => Couchbase.find[T](CappedBucket.docName, CappedBucket.viewName)(query)(bucket, r, ec).map(_.headOption))
  }

  def lastInsertedOption[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(true).setLimit(1)
    CappedBucket.trigger.flatMap(_ => Couchbase.find[T](CappedBucket.docName, CappedBucket.viewName)(query)(bucket, r, ec).map(_.headOption))
  }

  def tail[T](from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    CappedBucket.trigger.map( _ => Couchbase.tailableQuery[JsObject](CappedBucket.docName, CappedBucket.viewName, { obj =>
      (obj \ CappedBucket.cappedNaturalId).as[Long]
    }, from, every, unit)(bucket, CouchbaseRWImplicits.documentAsJsObjectReader, ec).through(Enumeratee.map { elem =>
       r.reads(elem.asInstanceOf[JsValue])
    }).through(Enumeratee.collect {
      case JsSuccess(elem, _) => elem
    }))
  }

  def insert[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    val jsObj = w.writes(value).as[JsObject]
    val enhancedJsObj = jsObj ++ Json.obj(CappedBucket.cappedRef -> true, CappedBucket.cappedNaturalId -> System.currentTimeMillis())
    CappedBucket.trigger.flatMap(_ => Couchbase.set[JsObject](key, enhancedJsObj, exp, persistTo, replicateTo)(bucket, CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  def insertWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    val jsObj = w.writes(value).as[JsObject]
    val enhancedJsObj = jsObj ++ Json.obj(CappedBucket.cappedRef -> true, CappedBucket.cappedNaturalId -> System.currentTimeMillis())
    CappedBucket.trigger.flatMap(_ => Couchbase.setWithKey[JsObject]({ _ => key(value)}, enhancedJsObj, exp, persistTo, replicateTo)(bucket, CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  def insertStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    val enhancedEnumerator = data.through(Enumeratee.map { elem =>
      val jsObj = w.writes(elem._2).as[JsObject]
      val enhancedJsObj = jsObj ++ Json.obj(CappedBucket.cappedRef -> true, CappedBucket.cappedNaturalId -> System.currentTimeMillis())
      (elem._1, enhancedJsObj)
    })
    CappedBucket.trigger.flatMap(_ => Couchbase.setStream[JsObject](enhancedEnumerator, exp, persistTo, replicateTo)(bucket, CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  def insertStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    val enhancedEnumerator = data.through(Enumeratee.map { elem =>
      val jsObj = w.writes(elem).as[JsObject]
      val enhancedJsObj = jsObj ++ Json.obj(CappedBucket.cappedRef -> true, CappedBucket.cappedNaturalId -> System.currentTimeMillis())
      (key(elem), enhancedJsObj)
    })
    CappedBucket.trigger.flatMap(_ => Couchbase.setStream[JsObject](enhancedEnumerator, exp, persistTo, replicateTo)(bucket, CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }
}
