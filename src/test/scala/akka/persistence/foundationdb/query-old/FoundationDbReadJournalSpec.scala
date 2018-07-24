//package akka.persistence.foundationdb.query
//
//import akka.Done
//import akka.actor.{ActorRef, ActorSystem, Props}
//import akka.persistence.foundationdb.TestActor
//import akka.persistence.foundationdb.query.scaladsl.FoundationDbReadJournal
//import akka.persistence.{DeleteMessagesSuccess, PersistentActor}
//import akka.persistence.journal.{Tagged, WriteEventAdapter}
//import akka.persistence.query.{EventEnvelope, NoOffset, PersistenceQuery}
//import akka.persistence.query.scaladsl.{CurrentEventsByTagQuery, EventsByTagQuery}
//import akka.stream.ActorMaterializer
//import akka.stream.testkit.scaladsl.TestSink
//import akka.testkit.{ImplicitSender, TestKit}
//import com.apple.foundationdb.FDB
//import com.typesafe.config.{Config, ConfigFactory}
//import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
//
//import scala.concurrent.duration._
//
//
//class FoundationDbReadJournalSpec extends TestKit(ActorSystem("FoundationDbReadJournalSpec", ConfigFactory.parseString(
//  """
//    |akka.persistence.journal.plugin = "foundationdb-journal"
//    |
//    |foundationdb-journal {
//    |
//    |      event-adapters {
//    |        color-tagger  = akka.persistence.foundationdb.ColorFruitTagger
//    |      }
//    |      event-adapter-bindings = {
//    |        "java.lang.String" = color-tagger
//    |      }
//    |
//    |}
//  """.stripMargin).withFallback(ConfigFactory.load())))
//  with ImplicitSender
//  with WordSpecLike
//  with Matchers with BeforeAndAfterAll {
//
//  implicit val mat = ActorMaterializer()
//
//
//  override protected def beforeAll(): Unit = {
//    val fdb = FDB.selectAPIVersion(510)
//
//    val db = fdb.open()
//
//    val journalConfig = new FoundationDbJournalConfig(system.settings.config.getConfig("foundationdb-journal"))
//
//
//    db.run { tr =>
//      tr.clear(journalConfig.tagsDir.range())
//      tr.clear(journalConfig.eventLogDir.range())
//      tr.clear(journalConfig.seqNoDir.range())
//      tr.clear(journalConfig.tagWatchDir.range())
//      tr.clear(journalConfig.pluginDirectory.range())
//      journalConfig.tagsDir.removeIfExists(db)
//      journalConfig.eventLogDir.removeIfExists(db)
//      journalConfig.seqNoDir.removeIfExists(db)
//      journalConfig.tagWatchDir.removeIfExists(db)
//      journalConfig.pluginDirectory.removeIfExists(db)
//    }
//
//
//    super.beforeAll()
//  }
//
//  val queries = PersistenceQuery(system).readJournalFor[FoundationDbReadJournal](FoundationDbReadJournal.Identifier)
//
//  val waitTime = 100.millis
//
//  "FoundationDb query currentEventsByTag" must {
//
//
//    "implement standard CurrentEventsByTagQuery" in {
//      queries.isInstanceOf[CurrentEventsByTagQuery] should ===(true)
//    }
//
//    "find existing events" in {
//      val a = system.actorOf(TestActor.props("a"))
//      val b = system.actorOf(TestActor.props("b"))
//      a ! "hello"
//      expectMsg(20.seconds, s"hello-done")
//      a ! "a green apple"
//      expectMsg(s"a green apple-done")
//      b ! "a black car"
//      expectMsg(s"a black car-done")
//      a ! "something else"
//      expectMsg(s"something else-done")
//      a ! "a green banana"
//      expectMsg(s"a green banana-done")
//      b ! "a green leaf"
//      expectMsg(s"a green leaf-done")
//
//      val greenSrc = queries.currentEventsByTag(tag = "green", offset = NoOffset)
//      val probe = greenSrc.runWith(TestSink.probe[Any])
//      probe.request(2)
//      probe.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
//      probe.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
//      probe.expectNoMessage(500.millis)
//      probe.request(2)
//      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
//      probe.expectComplete()
//
//      val blackSrc = queries.currentEventsByTag(tag = "black", offset = NoOffset)
//      val probe2 = blackSrc.runWith(TestSink.probe[Any])
//      probe2.request(5)
//      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "a black car") => e }
//      probe2.expectComplete()
//
//      val appleSrc = queries.currentEventsByTag(tag = "apple", offset = NoOffset)
//      val probe3 = appleSrc.runWith(TestSink.probe[Any])
//      probe3.request(5)
//      probe3.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
//      probe3.expectComplete()
//    }
//
//    "complete when no events" in {
//      val src = queries.currentEventsByTag(tag = "pink", offset = NoOffset)
//      val probe = src.runWith(TestSink.probe[Any])
//      probe.request(2)
//      probe.expectComplete()
//    }
//
//    "not see new events after demand request" in {
//      val c = system.actorOf(TestActor.props("c"))
//
//      val greenSrc = queries.currentEventsByTag(tag = "green", offset = NoOffset)
//      val probe = greenSrc.runWith(TestSink.probe[Any])
//      probe.request(2)
//      probe.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
//      probe.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
//      probe.expectNoMessage(waitTime)
//
//      c ! "a green cucumber"
//      expectMsg(s"a green cucumber-done")
//
//      probe.expectNoMessage(waitTime)
//      probe.request(5)
//      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
//      probe.expectComplete() // green cucumber not seen
//    }
//
//    "find events from versionsatamp offset" in {
//      val greenSrc1 = queries.currentEventsByTag(tag = "green", offset = NoOffset)
//      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
//      probe1.request(2)
//      val appleOffs = probe1.expectNextPF {
//        case e @ EventEnvelope(_, "a", 2L, "a green apple") => e
//      }.offset.asInstanceOf[VersionstampOffset]
//      val bananaOffs = probe1.expectNextPF {
//        case e @ EventEnvelope(_, "a", 4L, "a green banana") => e
//      }.offset.asInstanceOf[VersionstampOffset]
//      probe1.cancel()
//
//
//      appleOffs should be <= bananaOffs
//
//      val greenSrc2 = queries.currentEventsByTag(tag = "green", bananaOffs)
//      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
//      probe2.request(10)
//      if (appleOffs == bananaOffs)
//        probe2.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
//      probe2.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
//      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
//      probe2.cancel()
//    }
//
//    "find events from UUID offset" in {
//      val greenSrc1 = queries.currentEventsByTag(tag = "green", offset = NoOffset)
//      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
//      probe1.request(2)
//      probe1.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
//      val offs = probe1.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }.offset
//      probe1.cancel()
//
//      val greenSrc2 = queries.currentEventsByTag(tag = "green", offs)
//      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
//      probe2.request(10)
//      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
//      probe2.cancel()
//    }
//
//
//  }
//
//  "Cassandra live eventsByTag" must {
//    "implement standard EventsByTagQuery" in {
//      queries.isInstanceOf[EventsByTagQuery] should ===(true)
//    }
//
//    "find new events" in {
//      val d = system.actorOf(TestActor.props("d"))
//
//      val blackSrc = queries.eventsByTag(tag = "black", offset = NoOffset)
//      val probe = blackSrc.runWith(TestSink.probe[Any])
//      probe.request(2)
//      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "a black car") => e }
//      probe.expectNoMessage(waitTime)
//
//      d ! "a black dog"
//      expectMsg(s"a black dog-done")
//      d ! "a black night"
//      expectMsg(s"a black night-done")
//
//      probe.expectNextPF { case e @ EventEnvelope(_, "d", 1L, "a black dog") => e }
//      probe.expectNoMessage(waitTime)
//      probe.request(10)
//      probe.expectNextPF { case e @ EventEnvelope(_, "d", 2L, "a black night") => e }
//      probe.cancel()
//    }
//
//    "find events from timestamp offset" in {
//      val greenSrc1 = queries.eventsByTag(tag = "green", offset = NoOffset)
//      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
//      probe1.request(2)
//      val appleOffs = probe1.expectNextPF {
//        case e @ EventEnvelope(_, "a", 2L, "a green apple") => e
//      }.offset.asInstanceOf[VersionstampOffset]
//      val bananaOffs = probe1.expectNextPF {
//        case e @ EventEnvelope(_, "a", 4L, "a green banana") => e
//      }.offset.asInstanceOf[VersionstampOffset]
//      probe1.cancel()
//
//      appleOffs should be <= bananaOffs
//
//      val greenSrc2 = queries.eventsByTag(tag = "green", bananaOffs)
//      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
//      probe2.request(10)
//      if (appleOffs == bananaOffs)
//        probe2.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
//      probe2.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
//      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
//      probe2.expectNextPF { case e @ EventEnvelope(_, "c", 1L, "a green cucumber") => e }
//      probe2.expectNoMessage(waitTime)
//      probe2.cancel()
//    }
//
//    "find events from UUID offset " in {
//      val greenSrc1 = queries.eventsByTag(tag = "green", offset = NoOffset)
//      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
//      probe1.request(2)
//      probe1.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
//      val offs = probe1.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }.offset
//      probe1.cancel()
//
//      val greenSrc2 = queries.eventsByTag(tag = "green", offs)
//      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
//      probe2.request(10)
//      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
//      probe2.expectNextPF { case e @ EventEnvelope(_, "c", 1L, "a green cucumber") => e }
//      probe2.expectNoMessage(waitTime)
//      probe2.cancel()
//    }
//
//
//    "stream many events" in {
//      val e = system.actorOf(TestActor.props("e"))
//
//      val src = queries.eventsByTag(tag = "yellow", offset = NoOffset)
//      val probe = src.runWith(TestSink.probe[Any])
//
//      for (n <- 1 to 100)
//        e ! s"yellow-$n"
//
//      probe.request(200)
//      for (n <- 1 to 100) {
//        val Expected = s"yellow-$n"
//        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
//      }
//      probe.expectNoMessage(waitTime)
//
//      for (n <- 101 to 200)
//        e ! s"yellow-$n"
//
//      for (n <- 101 to 200) {
//        val Expected = s"yellow-$n"
//        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
//      }
//      probe.expectNoMessage(waitTime)
//
//      probe.request(10)
//      probe.expectNoMessage(waitTime)
//    }
//  }
//}
//
//
