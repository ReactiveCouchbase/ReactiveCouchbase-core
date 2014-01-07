package org.reactivecouchbase.client

import play.api.libs.json.{Json, JsObject}
import net.spy.memcached.ops.OperationStatus
import java.lang.RuntimeException

/**
 *
 * When the JSON format isn't good
 *
 * @param message
 * @param errors
 */
class JsonValidationException(message: String, errors: JsObject) extends ReactiveCouchbaseException("Json Validation failed", message + " : " + Json.stringify(errors))

/**
 *
 * When a Couchbase operation fails
 *
 * @param status
 */
class OperationFailedException(status: OperationStatus) extends ReactiveCouchbaseException("Operation failed", status.getMessage)

/**
 *
 * Standard ReactiveCouchbase Exception
 *
 * @param title
 * @param message
 */
class ReactiveCouchbaseException(title: String, message: String) extends RuntimeException(title + " : " + message)