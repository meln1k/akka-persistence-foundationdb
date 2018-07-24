package akka.persistence.foundationdb

import akka.actor.ActorSystem
import com.apple.foundationdb.{Cluster, Database, FDB}
import com.typesafe.config.Config

import scala.concurrent.{Future, blocking}

trait SessionProvider {
  def connect(): Future[Database]
}

object SessionProvider {

  private lazy val fdb: FDB = blocking(FDB.selectAPIVersion(510))

  private lazy val cluster: Cluster = blocking {
    val cluster = fdb.createCluster()
    sys.addShutdownHook {
      cluster.close()
      fdb.stopNetwork()
    }
    cluster
  }

  def apply(system: ActorSystem, config: Config): SessionProvider = {
    val blockingDispatcher = system.dispatchers.lookup("foundationdb-plugin-blocking-io-dispatcher")
    new SessionProvider {
      override def connect(): Future[Database] = {
        Future(cluster.openDatabase())(blockingDispatcher)
      }
    }
  }
}
