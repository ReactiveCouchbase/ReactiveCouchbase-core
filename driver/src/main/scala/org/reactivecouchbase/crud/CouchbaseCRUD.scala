package org.reactivecouchbase.crud

import play.api.libs.json._
import org.reactivecouchbase.{CouchbaseRWImplicits, CouchbaseBucket}
import scala.concurrent.{Future, ExecutionContext}
import java.util.UUID
import com.couchbase.client.protocol.views.{Query, View}
import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import org.reactivecouchbase.client.TypedRow

trait ReactiveCRUD[T] {

  import org.reactivecouchbase.CouchbaseRWImplicits._
  
  implicit def ctx: ExecutionContext// = implicitly[ExecutionContext]

  def bucket: CouchbaseBucket
  
  def format: Format[T]
  
  def idKey: String = "_id"

  def insert(t: T): Future[String] = {
    val id: String = UUID.randomUUID().toString
    val json = format.writes(t).as[JsObject]
    json \ idKey match {
      case _: JsUndefined => {
        val newJson = json ++ Json.obj(idKey -> JsString(id))
        bucket.set(id, newJson)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(_ => id)(ctx)
      }
      case actualId: JsString => {
        bucket.set(actualId.value, json)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(_ => actualId.value)(ctx)
      }
      case _ => throw new RuntimeException(s"Field with $idKey already exists and not of type JsString")
    }
  }

  def get(id: String): Future[Option[(T, String)]] = {
    bucket.get[T]( id )(format, ctx).map( _.map( v => ( v, id ) ) )(ctx)
  }

  def delete(id: String): Future[Unit] = {
    bucket.delete(id)(ctx).map(_ => ())
  }

  def update(id: String, t: T): Future[Unit] = {
    bucket.replace(id, t)(format, ctx).map(_ => ())
  }

  def updatePartial(id: String, upd: JsObject): Future[Unit] = {
    get(id).flatMap { opt =>
      opt.map { t =>
        val json = Json.toJson(t._1)(format).as[JsObject]
        val newJson = json.deepMerge(upd)
        bucket.replace((json \ idKey).as[JsString].value, newJson)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(_ => ())
      }.getOrElse(throw new RuntimeException(s"Cannot find id $id"))
    }
  }

  def batchInsert(elems: Enumerator[T]): Future[Int] = {
    elems(Iteratee.foldM[T, Int](0)( (s, t) => insert(t).map(_ => s + 1))).flatMap(_.run)
  }

  def find(sel: (View, Query), limit: Int = 0, skip: Int = 0): Future[Seq[(T, String)]] = {
    var query = sel._2
    if (limit != 0) query = query.setLimit(limit)
    if (skip != 0) query = query.setSkip(skip)
    bucket.search[JsObject](sel._1)(query)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).toList(ctx).map { l =>
      l.map { i =>
        val t = format.reads(i.document) match {
          case e:JsError => throw new RuntimeException("Document does not match object")
          case s:JsSuccess[T] => s.get
        }
        i.document \ idKey match {
          case actualId: JsString => (t, actualId.value)
          case _ => (t, i.id.get)
        }
      }
    }
  }

  def findStream(sel: (View, Query), skip: Int = 0, pageSize: Int = 0): Enumerator[Iterator[(T, String)]] = {
    var query = sel._2
    if (skip != 0) query = query.setSkip(skip)
    val futureEnumerator = bucket.search[JsObject](sel._1)(query)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).toList(ctx).map { l =>
      val size = if(pageSize != 0) pageSize else l.size
      Enumerator.enumerate(l.map { i =>
        val t = format.reads(i.document) match {
          case e:JsError => throw new RuntimeException("Document does not match object")
          case s:JsSuccess[T] => s.get
        }
        i.document \ idKey match {
          case actualId: JsString => (t, actualId.value)
          case _ => (t, i.id.get)
        }
      }.grouped(size).map(_.iterator))
    }
    Enumerator.flatten(futureEnumerator)
  }

  def batchDelete(sel: (View, Query)): Future[Unit] = {
    val extract = { tr: TypedRow[JsObject] => tr.id.get }
    bucket.search[JsObject](sel._1)(sel._2)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).enumerate.map { enumerator =>
      bucket.deleteStreamWithKey[TypedRow[JsObject]](extract, enumerator)(ctx)
    }.map(_ => ())
  }

  def batchUpdate(sel: (View, Query), upd: JsObject): Future[Unit] = {
    bucket.search[T](sel._1)(sel._2)(format, ctx).enumerate.map { enumerator =>
      bucket.replaceStream(enumerator.through(Enumeratee.map { t =>
        val json = Json.toJson(t.document)(format).as[JsObject]
        (t.id.get, json.deepMerge(upd))
      }))(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(_ => ())
    }
  }

  private def view(docName: String, viewName: String): Future[View] = {
    bucket.view(docName, viewName)(ctx)
  }
}