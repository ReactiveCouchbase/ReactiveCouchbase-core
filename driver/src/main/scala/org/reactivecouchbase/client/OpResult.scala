package org.reactivecouchbase.client

import play.api.libs.json.JsValue
import net.spy.memcached.ops.OperationStatus

case class OpResult(ok: Boolean, msg: Option[String], document: Option[JsValue], updated: Int, originalOperationStatus: Option[OperationStatus]) {
  def isSuccess = ok
  def isFailure = !ok
  def getMessage = msg.getOrElse("No message !!!")
  def getMessage(mess: String) = msg.getOrElse(mess)
}

object OpResult {

  def apply(status: OperationStatus) = {
    new OpResult(status.isSuccess, Some(status.getMessage), None, 0, Some(status))
  }

  def apply(status: OperationStatus, updated: Int) = {
    new OpResult(status.isSuccess, Some(status.getMessage), None, updated, Some(status))
  }

  def apply(status: OperationStatus, doc: Option[JsValue]) = {
    new OpResult(status.isSuccess, Some(status.getMessage), doc, 0, Some(status))
  }

  def apply(status: OperationStatus, updated: Int, doc: Option[JsValue] = None) = {
    new OpResult(status.isSuccess, Some(status.getMessage), doc, updated, Some(status))
  }
}
