/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

class CassandraReadJournalConfigSpec extends TestKit(ActorSystem("CassandraReadJournalConfigSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override protected def afterAll(): Unit = shutdown()

  "Cassandra read journal config" must {


  }
}
