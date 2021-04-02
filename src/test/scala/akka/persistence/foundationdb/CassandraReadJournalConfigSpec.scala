/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.foundationdb

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.must.Matchers

class CassandraReadJournalConfigSpec
    extends TestKit(ActorSystem("CassandraReadJournalConfigSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = shutdown()

  "Cassandra read journal config" must {}
}
