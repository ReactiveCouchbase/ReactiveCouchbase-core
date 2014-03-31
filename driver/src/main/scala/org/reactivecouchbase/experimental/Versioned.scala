package org.reactivecouchbase.experimental

case class VersionedDoc[T](version: Long, document: T)

object Versioned {}

