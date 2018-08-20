FoundationDb Plugins for Akka Persistence
======================================

Replicated [Akka Persistence](https://doc.akka.io/docs/akka/current/scala/persistence.html) journal and snapshot store backed by [FoundationDB](https://www.foundationdb.org/).

Dependencies
------------

### Latest release

To include the latest release of the FoundationDB plugins for **Akka 2.5.x** into your `sbt` project, add the following lines to your `build.sbt` file:

    libraryDependencies += Seq(
      "com.github.meln1k" %% "akka-persistence-foundationdb" % "0.1",
    )

This version of `akka-persistence-foundationdb` depends on Akka 2.5.13. It has been published for Scala 2.12.

Journal plugin
--------------

### Features

- All operations required by the Akka Persistence [journal plugin API](https://doc.akka.io/docs/akka/current/scala/persistence.html#journal-plugin-api) are fully supported.
- [Persistence Query](https://doc.akka.io/docs/akka/current/scala/persistence-query.html) support by `FoundationDbReadJournal`
- **Extra safe mode** which disallows journal corruption is enabled by default.
- Fast and efficient deletions

### Configuration

To activate the journal plugin, add the following line to your Akka `application.conf`:

    akka.persistence.journal.plugin = "foundationdb-journal"

This will run the journal with its default settings. The default settings can be changed with the configuration properties defined in [reference.conf](https://github.com/meln1k/akka-persistence-foundationdb/blob/master/src/main/resources/reference.conf):

### Caveats

- Detailed tests under failure conditions are still missing.
- Some tests are still missing.


These issues are likely to be resolved in future versions of the plugin.

Snapshot store plugin
---------------------

### Features

- Implements the Akka Persistence [snapshot store plugin API](https://doc.akka.io/docs/akka/current/scala/persistence.html#snapshot-store-plugin-api).

### Configuration

To activate the snapshot-store plugin, add the following line to your Akka `application.conf`:

    akka.persistence.snapshot-store.plugin = "foundationdb-snapshot-store"

This will run the snapshot store with its default settings. The default settings can be changed with the configuration properties defined in [reference.conf](https://github.com/meln1k/akka-persistence-foundationdb/blob/master/src/main/resources/reference.conf):

Persistence Queries
-------------------

It implements the following [Persistence Queries](https://doc.akka.io/docs/akka/current/scala/persistence-query.html):

* currentPersistenceIds
* eventsByPersistenceId, currentEventsByPersistenceId
* eventsByTag, currentEventsByTag

All live queries are implemented using database push mechanism and don't use any polling at all. It allows very low 
latencies between the writing event to the journal and it's replication to the query side.

Persistence Query usage example to obtain a stream with all events tagged with "someTag" with Persistence Query:

    val queries = PersistenceQuery(system).readJournalFor[FoundationDbReadJournal](FoundationDbReadJournal.Identifier)
    queries.eventsByTag("someTag", Offset.noOffset)
    
Compared to other journals, there are no limits regarding the amount of tags per event. Feel free to use as many as you wish!
