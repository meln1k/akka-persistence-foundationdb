package akka.persistence.foundationdb

import com.apple.foundationdb.directory.DirectorySubspace

case class Directories(
  plugin: DirectorySubspace,
  messages: DirectorySubspace,
  maxSeqNr: DirectorySubspace,
  tags: DirectorySubspace,
  tagWatches: DirectorySubspace,
  snapshots: DirectorySubspace
)
