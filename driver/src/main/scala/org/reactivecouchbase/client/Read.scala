package org.reactivecouchbase.client

import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import org.reactivecouchbase.client.CouchbaseFutures._
import net.spy.memcached.transcoders.Transcoder
import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import play.api.libs.json._
import collection.JavaConversions._

/**
 * Trait for read operations
 */
trait Read {

  def keyStats(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Map[String, String]] = {
    waitForOperation( bucket.couchbaseClient.getKeyStats(key), bucket, ec ).map(_.toMap)
  }

  def get[T](key: String, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[T]] = {
    waitForGet[T]( bucket.couchbaseClient.asyncGet(key, tc), bucket, ec ) map {
      case value: T => Some[T](value)
      case _ => None
    }
  }

  def rawFetch(keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[(String, String)] = {
    QueryEnumerator(keysEnumerator.apply(Iteratee.getChunks[String]).flatMap(_.run).flatMap { keys =>
      waitForBulkRaw( bucket.couchbaseClient.asyncGetBulk(keys), bucket, ec ).map { results =>
        Enumerator.enumerate(results.toList)
      }.map { enumerator =>
        enumerator &> Enumeratee.collect[(String, AnyRef)] { case (k: String, v: String) => (k, v) }
      }
    })
  }

  def fetch[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = {
    QueryEnumerator(rawFetch(keysEnumerator)(bucket, ec).enumerate.map { enumerator =>
      enumerator &> Enumeratee.map[(String, String)]( t => (t._1, r.reads(Json.parse(t._2))) ) &> Enumeratee.collect[(String, JsResult[T])] {
        case (k: String, JsSuccess(value, _)) => (k, value)
        case (k: String, JsError(errors)) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(errors))
      }
    })
  }

  def fetchValues[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(fetch[T](keysEnumerator)(bucket, r, ec).enumerate.map { enumerator =>
      enumerator &> Enumeratee.map[(String, T)](_._2)
    })
  }

  def fetch[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = {
    fetch[T](Enumerator.enumerate(keys))(bucket, r, ec)
  }

  def fetchValues[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    fetchValues[T](Enumerator.enumerate(keys))(bucket, r, ec)
  }

  def get[T](key: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    fetchValues[T](Enumerator(key))(bucket, r, ec).headOption(ec)
  }

  def getWithKey[T](key: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Option[(String, T)]] = {
    fetch[T](Enumerator(key))(bucket, r, ec).headOption(ec)
  }

  def fetchWithKeys[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    fetch[T](keys)(bucket, r, ec).toList(ec).map(_.toMap)
  }

  def fetchWithKeys[T](keys: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    fetch[T](keys)(bucket, r, ec).toList(ec).map(_.toMap)
  }

}
