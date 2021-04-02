/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 * Copyright (C) 2018 Nikita Melkozerov. <n.melkozerov at gmail dot com>
 */

package akka.persistence.foundationdb

import scala.collection.immutable
import scala.concurrent.duration._
import java.net.InetSocketAddress
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.must.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import scala.util.Random
import org.scalatest.wordspec.AnyWordSpecLike
import akka.testkit.TestKit
import akka.actor.ActorSystem
import scala.concurrent.Await
import scala.concurrent.Future
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext
import org.scalatest.BeforeAndAfterAll

object CassandraPluginConfigSpec {
//  class TestContactPointsProvider(system: ActorSystem, config: Config) extends ConfigSessionProvider(system, config) {
//    override def lookupContactPoints(clusterId: String)(implicit ec: ExecutionContext): Future[immutable.Seq[InetSocketAddress]] = {
//      if (clusterId == "cluster1")
//        Future.successful(List(new InetSocketAddress("host1", 9041)))
//      else
//        Future.successful(List(new InetSocketAddress("host1", 9041), new InetSocketAddress("host2", 9042)))
//    }
//
//  }
}

class CassandraPluginConfigSpec
    extends TestKit(ActorSystem("CassandraPluginConfigSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  "A CassandraPluginConfig" should {}
}
