package org.reactivecouchbase.implicits

import scala.util.control.NoStackTrace
import scala.concurrent._
import play.api.libs.json._
import scala.util.Try

package object flatfutures {

  object EmptyOption extends RuntimeException("Current option is empty :'(") with NoStackTrace

  implicit final class futureOfOptionToFuture[A](future: Future[Option[A]]) {
    def flatten(implicit ec: ExecutionContext): Future[A] = {
      future.flatMap {
        case Some(something) => Future.successful(something)
        case None => Future.failed(EmptyOption)
      }
    }
    def flattenM(none: => Future[A])(implicit ec: ExecutionContext): Future[A] = {
      future.flatMap {
        case Some(something) => Future.successful(something)
        case None => none
      }
    }
    def flatten(none: => A)(implicit ec: ExecutionContext): Future[A] = {
      future.flatMap {
        case Some(something) => Future.successful(something)
        case None => Future.successful(none)
      }
    }
    def flattenM[O](some: A => Future[O])(none: => Future[O])(implicit ec: ExecutionContext): Future[O] = {
      future.flatMap {
        case Some(something) => some(something)
        case None => none
      }
    }
    def flatten[O](some: A => O)(none: => O)(implicit ec: ExecutionContext): Future[O] = {
      future.flatMap {
        case Some(something) => Future.successful(some(something))
        case None => Future.successful(none)
      }
    }
  }
}

package object options {

  implicit final class BetterOption[A](option: Option[A]) {
    def fold[O](some: A => O)(none: => O): O = option match {
      case Some(something) => some(something)
      case None => none
    }
    def fold(none: => A): A = option match {
      case Some(something) => something
      case None => none
    }
    def |(a: => A): A = option.getOrElse(a)
  }
}

package object json {

  implicit final class EnhancedJsObject(js: JsObject) {

    def str(key: String): Option[String] =
      (js \ key).asOpt[String]

    def int(key: String): Option[Int] =
      (js \ key).asOpt[Int]

    def long(key: String): Option[Long] =
      (js \ key).asOpt[Long]

    def boolean(key: String): Option[Boolean] =
      (js \ key).asOpt[Boolean]

    def obj(key: String): Option[JsObject] =
      (js \ key).asOpt[JsObject]

    def arr(key: String): Option[JsArray] =
      (js \ key).asOpt[JsArray]

    def cleanup = JsObject {
      js.fields collect {
        case (key, value) if value != JsNull => key -> value
      }
    }
  }

  implicit final class EnhancedJsValue(js: JsValue) {

    def str(key: String): Option[String] =
      js.asOpt[JsObject] flatMap { obj =>
        (obj \ key).asOpt[String]
      }

    def int(key: String): Option[Int] =
      js.asOpt[JsObject] flatMap { obj =>
        (obj \ key).asOpt[Int]
      }

    def long(key: String): Option[Long] =
      js.asOpt[JsObject] flatMap { obj =>
        (obj \ key).asOpt[Long]
      }

    def obj(key: String): Option[JsObject] =
      js.asOpt[JsObject] flatMap { obj =>
        (obj \ key).asOpt[JsObject]
      }

    def arr(key: String): Option[JsArray] =
      js.asOpt[JsObject] flatMap { obj =>
        (obj \ key).asOpt[JsArray]
      }
  }
}

package object debug {

  implicit final class kcombine[A](a: A) {
    def combine(sideEffect: A => Unit): A = { sideEffect(a); a }
    def debug: A = debug(_ => s"[K-DEBUG] $a")
    def debug(out: A => String): A = {
      println(out(a))
      a
    }
  }

  implicit final class futureKcombine[A](fua: Future[A]) {
    def thenCombine(sideEffect: Try[A] => Unit)(implicit ec: ExecutionContext): Future[A] = {
      fua onComplete { case result => result.combine(sideEffect) }
      fua
    }
    def thenDebug(out: Try[A] => String)(implicit ec: ExecutionContext): Future[A] = {
      fua onComplete { case result => result.debug(out) }
      fua
    }
    def thenDebug(implicit ec: ExecutionContext): Future[A] = {
      fua onComplete { case result => result.debug }
      fua
    }
  }
}