Reactive Couchbase
=======================================

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/ReactiveCouchbase/ReactiveCouchbase-core?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Contents
--------

- [Basic Usage](#basic-usage)
    - [Project configuration](#project-configuration)
    - [Usage from a model](#standard-usage-from-a-model)
- [Capped bucket & tailable queries](#)
- [ReactiveCouchbase event store](#reactivecouchbase-event-store)
- [ReactiveCouchbase configuration cheatsheet](#reactivecouchbase-configuration-cheatsheet)

Current version
============

* current dev version is 0.1-SNAPSHOT
  * https://raw.github.com/ReactiveCouchbase/repository/master/snapshots

Starter Kits
=============

You can quickly bootstrap a project with the starter kit :

https://github.com/ReactiveCouchbase/reactivecouchbase-starter-kit

The binary is here :

* https://github.com/ReactiveCouchbase/repository/raw/master/starterkits/reactivecouchbase-starter-kit.zip

Just download the zip file, unzip it, change the app name/version in the `build.sbt` file and you're ready to go.

Basic Usage
============

Project configuration
---------------------

in your `build.sbt` file add dependencies and resolvers like :

```scala

name := "shorturls"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  cache,
  "org.reactivecouchbase" %% "reactivecouchbase-play" % "0.4-SNAPSHOT"
)

resolvers += "ReactiveCouchbase repository" at "https://raw.github.com/ReactiveCouchbase/repository/master/snapshots"

play.Project.playScalaSettings
```

or if you use the good old `project\Build.scala` file :


```scala

import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "shorturls"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    cache,
    "org.reactivecouchbase" %% "reactivecouchbase-play" % "0.4-SNAPSHOT"
  )

  val main = play.Project(appName, appVersion, appDependencies).settings(
    resolvers += "ReactiveCouchbase repository" at "https://raw.github.com/ReactiveCouchbase/repository/master/snapshots"
  )
}
```

then create a `conf/play.plugins` file and add :

`400:org.reactivecouchbase.play.plugins.CouchbasePlugin`

add in your `conf/application.conf` file :

```

couchbase {
  akka {
    timeout=1000
    execution-context {
      fork-join-executor {
        parallelism-factor = 4.0
        parallelism-max = 40
      }
    }
  }
  buckets = [{
    host="127.0.0.1"
    port="8091"
    base="pools"
    bucket="bucketname"
    user=""
    pass=""
    timeout="0"
  }]
}

```

You can of course connect many buckets with :

```
couchbase {

  ...

  buckets = [{
      host=["127.0.0.1", "192.168.0.42"]
      port="8091"
      base="pools"
      bucket="bucket1"
      user=""
      pass=""
      timeout="0"
  }, {
     host="127.0.0.1"
     port="8091"
     base="pools"
     bucket="bucket2"
     user=""
     pass=""
     timeout="0"
  }, {
     host="192.168.0.42"
     port="8091"
     base="pools"
     bucket="bucket3"
     user=""
     pass=""
     timeout="0"
  }]
}

```

then select one of them for each of your operation

Standard usage from a model
---------------------

```scala

// first import the implicit execution context  
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import play.api.libs.json._
import org.reactivecouchbase._
import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query}
import net.spy.memcached.ops.OperationStatus

case class Beer(id: String, name: String, brewery: String) {
  def save(): Future[OperationStatus] = Beer.save(this)
  def remove(): Future[OperationStatus] = Beer.remove(this)
}

object Beer {

  implicit val beerFmt = Json.format[Beer]

  // get a driver instance driver    
  val driver = ReactiveCouchbaseDriver()
  // get the default bucket  
  val bucket = driver.bucket("default")

  def findById(id: String): Future[Option[Beer]] = {
    bucket.get[Beer](id)
  }

  def findAll(): Future[List[Beer]] = {
    bucket.find[Beer]("beer", "by_name")(new Query().setIncludeDocs(true).setStale(Stale.FALSE))
  }

  def findByName(name: String): Future[Option[Beer]] = {
    val query = new Query().setIncludeDocs(true).setLimit(1)
          .setRangeStart(ComplexKey.of(name))
          .setRangeEnd(ComplexKey.of(s"$name\uefff").setStale(Stale.FALSE))
    bucket.find[Beer]("beer", "by_name")(query).map(_.headOption)
  }

  def save(beer: Beer): Future[OperationStatus] = {
    bucket.set[Beer](beer)
  }

  def remove(beer: Beer): Future[OperationStatus] = {
    bucket.delete[Beer](beer)
  }
}

```

You'll also need to call `driver.shutdown()` add the end of the application.

Capped buckets and tailable queries
====================================

ReactiveCouchbase provides a way to simulate capped buckets (http://docs.mongodb.org/manual/core/capped-collections/). 
You can see a capped bucket as a circular buffer. Once the buffer is full, the oldest entry is removed from the bucket.

Here, the bucket isn't really capped at couchbase level. It is capped at ReactiveCouchbase level.
You can use a bucket as a capped bucket using :

```scala
// first import the implicit execution context  
import scala.concurrent.ExecutionContext.Implicits.global

val driver = ReactiveCouchbaseDriver()
def bucket = driver.cappedBucket("default", 100) // here I use the default bucket as a capped bucket of size 100
```

of course, only data inserted with this `CappedBucket` object are considered for capped bucket features.

```scala

val john = Json.obj("name" -> "John", "fname" -> "Doe")

for (i <- 0 to 200) {
    bucket.insert(UUID.randomUUID().toString, john)
}
// still 100 people in the bucket (and possibly other data inserted with standard API)
```

When a json object is inserted, a timestamp is add to the object and this timestamp will be used to manage all the capped bucket features.

The nice part with capped buckets is the `tail` function. It's like using a `tail -f`command on the datas of the capped bucket

```scala

val enumerator1 = bucket.tail[JsValue]()
val enumerator2 = bucket.tail[JsValue](1265457L) // start to read data from 1265457L timestamp
val enumerator3 = bucket.tail[JsValue](1265457L, 200, TimeUnit.MILLISECONDS) // update every 200 milliseconds
enumerator1.map( doc => println(Json.prettyPrint(doc)) )
```

ReactiveCouchbase N1QL search
=======================

N1QL is the Couchbase Query Language. The N1QL Developer Preview 2 (DP2) is a pre-beta release of the language and is available at

http://www.couchbase.com/communities/n1ql

The ReactiveCouchbase plugin offers a very experimental access to N1QL based on the N1QL DP1. As it is experimental, I can not ensure that this feature will not massively change and/or will be continued.

First setup your N1QL query server. Download it and expand it. Then connect it to your Couchbase server.

./cbq-engine -couchbase http://<coucbhase-server-name>:8091/`

Now you have to configure N1QL in you `conf/application.conf` file add :

```

couchbase {
   n1ql {
     host="127.0.0.1"
     port=8093
   }
}

```

And now you can use it from your application

```scala

// first import the implicit execution context  
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import play.api.libs.json._
import org.reactivecouchbase._
import org.reactivecouchbase.CouchbaseN1QL._
import play.api.libs.iteratee.{Enumerator, Enumeratee}

case class Person(fname: String, age: Int)

object N1QLQuerier {

  implicit val personFormat = Json.format[Person]

  val driver = ReactiveCouchbaseDriver()

  def find(age: Int) = {
    N1QL( s""" SELECT fname, age FROM tutorial WHERE age > ${age} """, driver )
                                                   .toList[Person].map { persons =>
      println(s"Persons older than ${age}", persons))
    }
  }
}

```

or use it the Enumerator way

```scala

// first import the implicit execution context  
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import play.api.libs.json._
import org.reactivecouchbase._
import org.reactivecouchbase.CouchbaseN1QL._
import play.api.libs.iteratee._

case class Person(fname: String, age: Int)

object N1QLQuerier {

  implicit val personFormat = Json.format[Person]

  val driver = ReactiveCouchbaseDriver()

  def find(age: Int) = {
    N1QL( s""" SELECT fname, age FROM tutorial WHERE age > ${age} """, driver )
                                    .enumerate[Person].map { enumerator =>
       (enumerator &>
        Enumeratee.collect[Person] { case p@Person(_, age) if age < 50 => p } ><>
        Enumeratee.map[Person](personFormat.writes)) >>>
        Enumerator.eof).apply(Iteratee.foreach { person =>
          println(person)
        })
    }
  }
}

```


ReactiveCouchbase Atomic operation
==========================

Couchbase manage lock system to allow you to perform some Atomic operation. This driver use the Actor system to perform atomic operation easily.

The AtomicTest test file give you an example about how to deal with it.


About Couchbase Expiration
===========================

Couchbase manage expiration of value with Int. Just import org.reactivecouchbase.CouchbaseExpiration._ to be able to manage expiration with Int or Duration. Due to couchbase [weird management of timestamp and duration](http://docs.couchbase.com/couchbase-sdk-java-1.0/#expiry-values) duration longer than 30 days will be converted to timestamp...


ReactiveCouchbase configuration cheatsheet
===================================

Here is the complete plugin configuration with default values

```

couchbase {
  akka {               # execution context configuration, optional
    timeout=1000                    # default timeout for futures (ms), optional
    execution-context {             # actual execution context configuration if needed, optional
      fork-join-executor {
        parallelism-factor = 4.0
        parallelism-max = 40
      }
    }
  }
  buckets = [{                      # available bucket. It's an array, so you can define multiple values
    host="127.0.0.1", "127.0.0.1"   # Couchbase hosts, can be multiple comma separated values
    port="8091"
    base="pools"
    bucket="$bucketname"
    pass="$password"
    timeout="0"
  }, {
    host="127.0.0.1", "127.0.0.1"
    port="8091"
    base="pools"
    bucket="$bucketname1"
    pass="$password"
    timeout="0"
  }]
  failfutures=false                 # fail Scala future if OperationStatus is failed, optional
  json {                            # JSON related configuration, optional
    validate=true                   # JSON structure validation fail
  }
  driver {                          # couchbase driver related config
    useec=true                      # use couchbase-executioncontext as ExecutorService for couchbase driver, optional
  }
  n1ql {                            # N1QL access from API, optionnal
    host="127.0.0.1"                # Host of the N1QL search server
    port=8093                       # Port of the N1QL search server
  }
}

```