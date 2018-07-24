package akka.persistence.foundationdb.query

import akka.actor.ActorSystem
import akka.persistence.foundationdb.FoundationDbPluginConfig
import com.typesafe.config.Config

class FoundationDbReadJournalConfig(system: ActorSystem, config: Config) extends FoundationDbPluginConfig(system, config) {



}
