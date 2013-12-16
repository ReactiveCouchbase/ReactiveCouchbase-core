package org.reactivecouchbase.client

import play.api.libs.json.{Json, JsObject}
import net.spy.memcached.ops.OperationStatus
import java.lang.RuntimeException

class JsonValidationException(message: String, errors: JsObject) extends RuntimeException(message + " : " + Json.stringify(errors))
class OperationFailedException(status: OperationStatus) extends RuntimeException(status.getMessage)
class ReactiveCouchbaseException(title: String, message: String) extends RuntimeException(title + " : " + message)