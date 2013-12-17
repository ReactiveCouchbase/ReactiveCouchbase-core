package org.reactivecouchbase.client

import org.reactivecouchbase.{StandaloneLogger, CouchbaseBucket}
import scala.concurrent.{ Future, ExecutionContext }
import net.spy.memcached.ops.OperationStatus
import org.reactivecouchbase.client.CouchbaseFutures._
import net.spy.memcached.CASValue
import play.api.libs.json._
import akka.actor.Actor
import akka.pattern._
import akka.actor.Props
import akka.util.Timeout
import scala.concurrent.duration._
import net.spy.memcached.CASResponse
import scala.util.{Failure, Success}
import akka.pattern.after
import scala.util.control.ControlThrowable
import org.reactivecouchbase.CouchbaseExpiration._

case class AtomicRequest[T](key: String, operation: T => T, bucket: CouchbaseBucket, atomic: Atomic, r: Reads[T], w: Writes[T], ec: ExecutionContext, numberTry: Int)

case class AtomicSucess[T](key: String)
case class AtomicError[T](request: AtomicRequest[T], message: String) extends ControlThrowable
case class AtomicTooMuchTryError[T](request: AtomicRequest[T]) extends ControlThrowable
case class AtomicNoKeyFoundError[T](request: AtomicRequest[T]) extends ControlThrowable
case class AtomicWeirdError() extends ControlThrowable

object AtomicActor {
  def props[T]: Props = Props(classOf[AtomicActor[T]])
}

class AtomicActor[T] extends Actor {

  def receive = {

    case ar: AtomicRequest[T] => {
      // I need some implicit, I know it's not good looking
      implicit val rr = ar.r
      implicit val ww = ar.w
      implicit val bb = ar.bucket
      implicit val ee = ar.ec
      // define other implicit
      implicit val timeout = Timeout(8 minutes)
      // backup my sender, need it later, and actor shared state is not helping...
      val sen = sender

      if (ar.numberTry < 15) {

        val myresult = ar.atomic.getAndLock(ar.key, 5 minutes).onComplete {
          case Success(Some(cas)) => {
            // \o/ we successfully lock the key
            // get current object
            val cv = ar.r.reads(Json.parse(cas.getValue.toString)).get
            // apply transformation and get new object
            val nv = ar.operation(cv)
            // write new object to couchbase and unlock :-)
            // TODO : use asyncCAS method, better io management
            val res = ar.bucket.couchbaseClient.cas(ar.key, cas.getCas, ar.w.writes(nv).toString)
            // reply to sender it's OK
            res match {
              case CASResponse.OK => sen ! Future.successful(AtomicSucess[T](ar.key))
              case CASResponse.NOT_FOUND => sen ! Future.failed(throw new AtomicNoKeyFoundError[T](ar))
              case CASResponse.EXISTS => {
                // something REALLY weird append during the locking time. But anyway, we can retry :-)
                bb.driver.logger.info("Couchbase lock WEIRD error : desync of CAS value. Retry anyway")
                val ar2 = AtomicRequest(ar.key, ar.operation, ar.bucket, ar.atomic, ar.r, ar.w, ar.ec, ar.numberTry + 1)
                val atomic_actor = bb.driver.system().actorOf(AtomicActor.props[T])
                for (
                  tr <- (atomic_actor ? ar2)
                ) yield (sen ! tr)
              }
              case _ => sen ! Future.failed(throw new AtomicError[T](ar, res.name))
            }
          }
          case Failure(ex: OperationStatusErrorNotFound) => sen ! Future.failed(new AtomicNoKeyFoundError[T](ar))
          case r => {
            // Too bad, the object is not locked and some else is working on it...
            // build a new atomic request
            val ar2 = AtomicRequest(ar.key, ar.operation, ar.bucket, ar.atomic, ar.r, ar.w, ar.ec, ar.numberTry + 1)
            // get my actor
            val atomic_actor = bb.driver.system().actorOf(AtomicActor.props[T])
            // wait and retry by asking actor with new atomic request
            for (
              d <- after(200 millis, using =
                context.system.scheduler)(Future.successful(ar2));
              tr <- (atomic_actor ? d)
            // send result :-)
            ) yield (sen ! tr)
          }
        }
      } else {
        sen ! Future.failed(throw new AtomicTooMuchTryError[T](ar))
      }
    }
    case _ => {
      // TODO : use current logger here
      StandaloneLogger.error("An atomic actor get a message, but not a atomic request, it's weird ! ")
      sender ! Future.failed(throw new AtomicWeirdError())
    }
  }
}

trait Atomic {

  def getAndLock[T](key: String, exp: CouchbaseExpirationTiming)(implicit r: Reads[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[CASValue[T]]] = {
    waitForGetAndCas[T](bucket.couchbaseClient.asyncGetAndLock(key, exp), bucket, ec, r) map {
      case value: CASValue[T] =>
        Some[CASValue[T]](value)
      case _ => None
    }
  }

  def unlock(key: String, casId: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus(bucket.couchbaseClient.asyncUnlock(key, casId), bucket, ec)
  }

  def atomicUpdate[T](key: String, operation: T => T)(implicit bucket: CouchbaseBucket, ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[Any] = {
    implicit val timeout = Timeout(8 minutes)
    val ar = AtomicRequest[T](key, operation, bucket, this, r, w, ec, 1)
    val atomic_actor = bucket.driver.system().actorOf(AtomicActor.props[T])
    (atomic_actor.ask(ar))
  }

}

