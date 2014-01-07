package org.reactivecouchbase

import play.api.libs.json._
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsObject

/**
 * A bunch of implicit Json readers and writers to deal with json docs as string and
 * json docs as JsValue or JsObject. Very useful to deal with 'raw' types
 *
 * Example :
 * {{{
 * import org.reactivecouchbase.CouchbaseRWImplicits._
 * }}}
 */
object CouchbaseRWImplicits {

  /**
   * Reads JsValue as a String
   */
  implicit val documentAsStringReader = new Reads[String] {
    def reads(json: JsValue): JsResult[String] = JsSuccess(Json.stringify(json))
  }

  /**
   * Reads JsValue as JsObject
   */
  implicit val documentAsJsObjectReader = new Reads[JsObject] {
    def reads(json: JsValue): JsResult[JsObject] = JsSuccess(json.as[JsObject])
  }

  /**
   * Reads JsValue as JsValue
   */
  implicit val documentAsJsValuetReader = new Reads[JsValue] {
    def reads(json: JsValue): JsResult[JsValue] = JsSuccess(json)
  }

  /**
   * Writes JsValue as String
   */
  implicit val stringToDocumentWriter = new Writes[String] {
    def writes(o: String): JsValue = Json.parse(o)
  }

  /**
   * Writes JsValue as JsObject
   */
  implicit val jsObjectToDocumentWriter = new Writes[JsObject] {
    def writes(o: JsObject): JsValue = o
  }

  /**
   * Writes JsValue as JsValue
   */
  implicit val jsValueToDocumentWriter = new Writes[JsValue] {
    def writes(o: JsValue): JsValue = o
  }
}
