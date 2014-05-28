package org.reactivecouchbase.client

import net.spy.memcached.internal._
import net.spy.memcached.CASValue
import scala.concurrent.{ Promise, Future, ExecutionContext }
import com.couchbase.client.internal.{ HttpCompletionListener, HttpFuture }
import net.spy.memcached.ops.OperationStatus
import play.api.libs.json.Reads
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

/**
 * Internal API to deal with Java Drivers Future
 */
private[reactivecouchbase] object CouchbaseFutures {

  def timeout[T](promise: Promise[T], f: java.util.concurrent.Future[_], complete: () => Unit)(bucket: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    if (bucket.doubleCheck) {
      def check: Unit = {
        if (!promise.isCompleted && (f.isDone || f.isCancelled)) complete()
        else if (!promise.isCompleted) bucket.cbDriver.scheduler().scheduleOnce(FiniteDuration(2, TimeUnit.SECONDS))(check)(ec)
      }
      bucket.cbDriver.scheduler().scheduleOnce(FiniteDuration(2, TimeUnit.SECONDS))(check)(ec)
    }
    if (bucket.enableOperationTimeout) {
      bucket.cbDriver.scheduler().scheduleOnce(FiniteDuration(bucket.ecTimeout, TimeUnit.MILLISECONDS)) {
        val done = promise tryFailure new java.util.concurrent.TimeoutException
        if (done) {
          bucket.cbDriver.logger.error("Couchbase operation timeout there, what happened ???")
        }
      }(ec)
      promise.future
    } else {
      promise.future
    }
  }

  def block[T](future: java.util.concurrent.Future[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    if (bucket.block) {
      bucket.cbDriver.logger.warn("Current thread blocking operation")
      try {
        Future.successful(future.get(bucket.ecTimeout, TimeUnit.MILLISECONDS))
      } catch {
        case e: Throwable => Future.failed(e)
      }
    } else {
      bucket.cbDriver.logger.warn("Pooled thread blocking operation")
      Future( future.get() )
    }
  }

  def oblock[T](future: HttpFuture[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OperationStatus] = {
    if (bucket.block) {
      bucket.cbDriver.logger.warn("Current thread blocking operation")
      try {
        future.get(bucket.ecTimeout, TimeUnit.MILLISECONDS)
        Future.successful(future.getStatus)
      } catch {
        case e: Throwable => Future.failed(e)
      }
    } else {
      bucket.cbDriver.logger.warn("Pooled thread blocking operation")
      Future({
        future.get()
        future.getStatus
      })
    }
  }

  def oblock[T](future: OperationFuture[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OperationStatus] = {
    if (bucket.block) {
      bucket.cbDriver.logger.warn("Current thread blocking operation")
      try {
        future.get(bucket.ecTimeout, TimeUnit.MILLISECONDS)
        Future.successful(future.getStatus)
      } catch {
        case e: Throwable => Future.failed(e)
      }
    } else {
      bucket.cbDriver.logger.warn("Pooled thread blocking operation")
      Future({
        future.get()
        future.getStatus
      })
    }
  }

  /**
   *
   * Transform an BulkFuture to a Future[Map]
   *
   * @param future the Java Driver Future
   * @param b the bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def waitForBulkRaw(future: BulkFuture[java.util.Map[String, AnyRef]], b: CouchbaseBucket, ec : ExecutionContext): Future[java.util.Map[String, AnyRef]] = {
    if (b.blockInFutures) return block(future)(b, ec)
    val promise = Promise[java.util.Map[String, AnyRef]]()
    def complete(): Unit = {
      val f = future
      if (b.failWithOpStatus && f.getStatus == null) {
        promise.tryFailure(new ReactiveCouchbaseException("Operation failed", "Unable to fetch last Couchbase operation status (Null value) ..."))
      } else if (b.failWithOpStatus && (!f.getStatus.isSuccess)) {
        promise.tryFailure(new OperationFailedException(f.getStatus))
      } else {
        //if (!f.getStatus.isSuccess) b.driver.logger.error(f.getStatus.getMessage)
        if (f.isDone || f.isCancelled || f.isTimeout) {
          promise.trySuccess(f.get()) //.asInstanceOf[java.util.Map[String, AnyRef]])
        } else {
          if (b.checkFutures) promise.tryFailure(new Throwable(s"BulkFuture epic fail !!! ${f.isDone} : ${f.isCancelled} : ${f.isTimeout}"))
          else {
            b.driver.logger.warn(s"BulkFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
            promise.trySuccess(f.get()) //.asInstanceOf[java.util.Map[String, AnyRef]])
          }
        }
      }
      if (!promise.isCompleted) b.logger.warn("BulkFuture didn't complete the promise, that's weird !!!")
    }
    future.addListener(new BulkGetCompletionListener() {
      def onComplete(f: BulkGetFuture[_]) = complete()
    })
    timeout( promise, future, () => complete() )( b, ec )
  }

  /**
   *
   * Transform an GetFuture to a Future[T]
   *
   * @param future the Java Driver Future
   * @param b the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T internal type
   * @return
   */
  def waitForGet[T](future: GetFuture[T], b: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    if (b.blockInFutures) return block(future)(b, ec)
    val promise = Promise[T]()
    def complete(): Unit = {
      val f = future
      if (b.failWithOpStatus && (!f.getStatus.isSuccess)) {
        promise.tryFailure(new OperationFailedException(f.getStatus))
      } else {
        //if (!f.getStatus.isSuccess) b.driver.logger.error(f.getStatus.getMessage)
        if (f.isDone || f.isCancelled) {
          promise.trySuccess(f.get()) //.asInstanceOf[T])
        } else {
          if (b.checkFutures) promise.tryFailure(new Throwable(s"GetFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
          else {
            b.driver.logger.warn(s"GetFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
            promise.trySuccess(f.get()) //.asInstanceOf[T])
          }
        }
      }
      if (!promise.isCompleted) b.logger.warn("GetFuture didn't complete the promise, that's weird !!!")
    }
    future.addListener(new GetCompletionListener() {
      def onComplete(f: GetFuture[_]) = complete()
    })
    timeout( promise, future, () => complete() )( b, ec )
  }

  /**
   *
   * @param opstat the operation status
   */
  class OperationStatusError(val opstat: OperationStatus) extends ReactiveCouchbaseException("OperationStatusError", opstat.getMessage)

  /**
   *
   * @param opstat the operation status
   */
  class OperationStatusErrorNotFound(val opstat: OperationStatus) extends ReactiveCouchbaseException("OperationStatusErrorNotFound", opstat.getMessage)

  /**
   *
   * @param opstat the operation status
   */
  class OperationStatusErrorIsLocked(val opstat: OperationStatus) extends ReactiveCouchbaseException("OperationStatusErrorIsLocked", opstat.getMessage)

  /**
   *
   * Transform an OperationFuture to a Future[CasValue[T]]
   *
   * @param future the Java Driver Future
   * @param b the bucket to use
   * @param ec ExecutionContext for async processing
   * @param r
   * @tparam T internal type
   * @return
   */
  def waitForGetAndCas[T](future: OperationFuture[CASValue[Object]], b: CouchbaseBucket, ec: ExecutionContext, r: Reads[T]): Future[CASValue[T]] = {
    val promise = Promise[CASValue[T]]()
    def complete(): Unit = {
      val f = future
      if (!f.getStatus.isSuccess) {
        b.driver.logger.error(f.getStatus.getMessage + " for key " + f.getKey)
        f.getStatus.getMessage match {
          case "NOT_FOUND" => promise.tryFailure(new OperationStatusErrorNotFound(f.getStatus))
          case "LOCK_ERROR" => promise.tryFailure(new OperationStatusErrorIsLocked(f.getStatus))
          case _ => promise.tryFailure(new OperationStatusError(f.getStatus))
        }
      } else if (f.isDone || f.isCancelled) {
        promise.trySuccess(f.get().asInstanceOf[CASValue[T]])
      } else {
        if (b.checkFutures) promise.tryFailure(new Throwable(s"GetFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
        else {
          b.driver.logger.warn(s"GetFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
          promise.trySuccess(f.get().asInstanceOf[CASValue[T]])
        }
      }
      if (!promise.isCompleted) b.logger.warn("CasFuture didn't complete the promise, that's weird !!!")
    }
    future.addListener(new OperationCompletionListener() {
      def onComplete(f: OperationFuture[_]) = complete()
    })
    timeout( promise, future, () => complete() )( b, ec )
  }

  /**
   *
   * Transform an HttpFuture to a  Future[OperationStatus]
   *
   * @param future the Java Driver Future
   * @param b the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T internal type
   * @return
   */
  def waitForHttpStatus[T](future: HttpFuture[T], b: CouchbaseBucket, ec: ExecutionContext):  Future[OperationStatus] = {
    if (b.blockInFutures) return oblock(future)(b, ec)
    val promise = Promise[OperationStatus]()
    def complete(): Unit = {
      val f = future
      if (b.failWithOpStatus && (!f.getStatus.isSuccess)) {
        promise.tryFailure(new OperationFailedException(f.getStatus))
      } else {
        //if (!f.getStatus.isSuccess) b.driver.logger.error(f.getStatus.getMessage)
        if (f.isDone || f.isCancelled) {
          promise.trySuccess(f.getStatus)
        } else {
          if (b.checkFutures) promise.tryFailure(new Throwable(s"HttpFutureStatus epic fail !!! ${f.isDone} : ${f.isCancelled}"))
          else {
            b.driver.logger.warn(s"HttpFutureStatus not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
            promise.trySuccess(f.getStatus)
          }
        }
      }
      if (!promise.isCompleted) b.logger.warn("HttpFuture didn't complete the promise, that's weird !!!")
    }
    future.addListener(new HttpCompletionListener() {
      def onComplete(f: HttpFuture[_]) = complete()
    })
    timeout( promise, future, () => complete() )( b, ec )
  }

  /**
   *
   * Transform an HttpFuture to a Future[T]
   *
   * @param future the Java Driver Future
   * @param b the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T internal type
   * @return
   */
  def waitForHttp[T](future: HttpFuture[T], b: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    if (b.blockInFutures) return block(future)(b, ec)
    val promise = Promise[T]()
    def complete(): Unit = {
      val f = future
      if (b.failWithOpStatus && (!f.getStatus.isSuccess)) {
        promise.tryFailure(new OperationFailedException(f.getStatus))
      } else {
        //if (!f.getStatus.isSuccess) b.driver.logger.error(f.getStatus.getMessage)
        if (f.isDone || f.isCancelled) {
          promise.trySuccess(f.get()) //.asInstanceOf[T])
        } else {
          if (b.checkFutures) promise.tryFailure(new Throwable(s"HttpFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
          else {
            b.driver.logger.warn(s"HttpFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
            promise.trySuccess(f.get()) //.asInstanceOf[T])
          }
        }
      }
      if (!promise.isCompleted) b.logger.warn("HttpFuture didn't complete the promise, that's weird !!!")
    }
    future.addListener(new HttpCompletionListener() {
      def onComplete(f: HttpFuture[_]) = complete()
    })
    timeout( promise, future, () => complete() )( b, ec )
  }

  /**
   *
   * Transform an OperationFuture to a  Future[OperationStatus]
   *
   * @param future the Java Driver Future
   * @param b the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T internal type
   * @return the Scala Future
   */
  def waitForOperationStatus[T](future: OperationFuture[T], b: CouchbaseBucket, ec: ExecutionContext):  Future[OperationStatus] = {
    if (b.blockInFutures) return oblock(future)(b, ec)
    val promise = Promise[OperationStatus]()
    def complete(): Unit = {
      val f = future
      if (b.failWithOpStatus && (!f.getStatus.isSuccess)) {
        promise.tryFailure(new OperationFailedException(f.getStatus))
      } else {
        //if (!f.getStatus.isSuccess) b.driver.logger.error(f.getStatus.getMessage)
        if (f.isDone || f.isCancelled) {
          promise.trySuccess(f.getStatus)
        } else {
          if (b.checkFutures) promise.tryFailure(new Throwable(s"OperationFutureStatus epic fail !!! ${f.isDone} : ${f.isCancelled}"))
          else {
            b.driver.logger.warn(s"OperationFutureStatus not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
            promise.trySuccess(f.getStatus)
          }
        }
      }
      if (!promise.isCompleted) b.logger.warn("OperationFuture didn't complete the promise, that's weird !!!")
    }
    future.addListener(new OperationCompletionListener() {
      def onComplete(f: OperationFuture[_]) = complete()
    })
    timeout( promise, future, () => complete() )( b, ec )
  }

  /**
   *
   * Transform an OperationFuture to a Future[T]
   *
   * @param future the Java Driver Future
   * @param b the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T internal type
   * @return
   */
  def waitForOperation[T](future: OperationFuture[T], b: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    if (b.blockInFutures) return block(future)(b, ec)
    val promise = Promise[T]()
    def complete(): Unit = {
      val f = future
      if (b.failWithOpStatus && (!f.getStatus.isSuccess)) {
        promise.tryFailure(new OperationFailedException(f.getStatus))
      } else {
        //if (!f.getStatus.isSuccess) b.driver.logger.error(f.getStatus.getMessage)
        if (f.isDone || f.isCancelled) {
          promise.trySuccess(f.get()) //.asInstanceOf[T])
        } else {
          if (b.checkFutures) promise.tryFailure(new Throwable(s"OperationFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
          else {
            b.driver.logger.warn(s"OperationFuture not completed yet, success anyway : ${f.isDone} : ${f.isCancelled}")
            promise.trySuccess(f.get()) //.asInstanceOf[T])
          }
        }
      }
      if (!promise.isCompleted) b.logger.warn("OperationFuture didn't complete the promise, that's weird !!!")
    }
    future.addListener(new OperationCompletionListener() {
      def onComplete(f: OperationFuture[_]) = complete()
    })
    timeout( promise, future, () => complete() )( b, ec )
  }
}
