package org.reactivecouchbase.client

import net.spy.memcached.internal._
import net.spy.memcached.CASValue
import scala.concurrent.{ Promise, Future, ExecutionContext }
import com.couchbase.client.internal.{ HttpCompletionListener, HttpFuture }
import net.spy.memcached.ops.OperationStatus
import play.api.libs.json.Reads
import org.reactivecouchbase.{Configuration, Logger}

object CouchbaseFutures {

  val logger = Logger
  val checkFutures = Configuration.getBoolean("couchbase.driver.checkfuture").getOrElse(false)

  def waitForBulkRaw(future: BulkFuture[java.util.Map[String, AnyRef]], ec: ExecutionContext): Future[java.util.Map[String, AnyRef]] = {
    val promise = Promise[java.util.Map[String, AnyRef]]()
    future.addListener(new BulkGetCompletionListener() {
      def onComplete(f: BulkGetFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) logger.error(f.getStatus.getMessage)
          if (f.isDone || f.isCancelled || f.isTimeout) {
            promise.success(f.get().asInstanceOf[java.util.Map[String, AnyRef]])
          } else {
            if (checkFutures) promise.failure(new Throwable(s"BulkFuture epic fail !!! ${f.isDone} : ${f.isCancelled} : ${f.isTimeout}"))
            else {
              logger.info(s"BulkFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
              promise.success(f.get().asInstanceOf[java.util.Map[String, AnyRef]])
            }
          }
        }
      }
    })
    promise.future
  }

  def waitForGet[T](future: GetFuture[T], ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    future.addListener(new GetCompletionListener() {
      def onComplete(f: GetFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) logger.error(f.getStatus.getMessage)
          if (f.isDone || f.isCancelled) {
            promise.success(f.get().asInstanceOf[T])
          } else {
            if (checkFutures) promise.failure(new Throwable(s"GetFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
            else {
              logger.info(s"GetFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
              promise.success(f.get().asInstanceOf[T])
            }
          }
        }
      }
    })
    promise.future
  }

  class OperationStatusError(val opstat: OperationStatus) extends Throwable
  class OperationStatusErrorNotFound(val opstat: OperationStatus) extends Throwable
  class OperationStatusErrorIsLocked(val opstat: OperationStatus) extends Throwable

  def waitForGetAndCas[T](future: OperationFuture[CASValue[Object]], ec: ExecutionContext, r: Reads[T]): Future[CASValue[T]] = {
    val promise = Promise[CASValue[T]]()
    future.addListener(new OperationCompletionListener() {
      def onComplete(f: OperationFuture[_]) = {
        if (!f.getStatus.isSuccess) {
          logger.error(f.getStatus.getMessage + " for key " + f.getKey)
          f.getStatus.getMessage match {
            case "NOT_FOUND" => promise.failure(new OperationStatusErrorNotFound(f.getStatus))
            case "LOCK_ERROR" => promise.failure(new OperationStatusErrorIsLocked(f.getStatus))
            case _ => promise.failure(new OperationStatusError(f.getStatus))
          }
        } else if (f.isDone || f.isCancelled) {
          promise.success(f.get().asInstanceOf[CASValue[T]])
        } else {
          if (checkFutures) promise.failure(new Throwable(s"GetFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
          else {
            logger.info(s"GetFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
            promise.success(f.get().asInstanceOf[CASValue[T]])
          }
        }

      }
    })
    promise.future
  }

  def waitForHttpStatus[T](future: HttpFuture[T], ec: ExecutionContext): Future[OperationStatus] = {
    val promise = Promise[OperationStatus]()
    future.addListener(new HttpCompletionListener() {
      def onComplete(f: HttpFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) logger.error(f.getStatus.getMessage)
          if (f.isDone || f.isCancelled) {
            promise.success(f.getStatus)
          } else {
            if (checkFutures) promise.failure(new Throwable(s"HttpFutureStatus epic fail !!! ${f.isDone} : ${f.isCancelled}"))
            else {
              logger.info(s"HttpFutureStatus not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
              promise.success(f.getStatus)
            }
          }
        }
      }
    })
    promise.future
  }

  def waitForHttp[T](future: HttpFuture[T], ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    future.addListener(new HttpCompletionListener() {
      def onComplete(f: HttpFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) logger.error(f.getStatus.getMessage)
          if (f.isDone || f.isCancelled) {
            promise.success(f.get().asInstanceOf[T])
          } else {
            if (checkFutures) promise.failure(new Throwable(s"HttpFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
            else {
              logger.info(s"HttpFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
              promise.success(f.get().asInstanceOf[T])
            }
          }
        }
      }
    })
    promise.future
  }

  def waitForOperationStatus[T](future: OperationFuture[T], ec: ExecutionContext): Future[OperationStatus] = {
    val promise = Promise[OperationStatus]()
    future.addListener(new OperationCompletionListener() {
      def onComplete(f: OperationFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) logger.error(f.getStatus.getMessage)
          if (f.isDone || f.isCancelled) {
            promise.success(f.getStatus)
          } else {
            if (checkFutures) promise.failure(new Throwable(s"OperationFutureStatus epic fail !!! ${f.isDone} : ${f.isCancelled}"))
            else {
              logger.info(s"OperationFutureStatus not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
              promise.success(f.getStatus)
            }
          }
        }
      }
    })
    promise.future
  }

  def waitForOperation[T](future: OperationFuture[T], ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    future.addListener(new OperationCompletionListener() {
      def onComplete(f: OperationFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) logger.error(f.getStatus.getMessage)
          if (f.isDone || f.isCancelled) {
            promise.success(f.get().asInstanceOf[T])
          } else {
            if (checkFutures) promise.failure(new Throwable(s"OperationFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
            else {
              logger.info(s"OperationFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
              promise.success(f.get().asInstanceOf[T])
            }
          }
        }
      }
    })
    promise.future
  }
}
