package org.reactivecouchbase.experimental

import play.api.libs.json.Json

trait Versioned {



}

case class VersionedDoc[T](version: Long, document: T)

object Versioned {
  //implicit val versionedDocumnetFormat = Json.format[VersionedDoc]
}
