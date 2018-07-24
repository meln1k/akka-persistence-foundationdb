package akka.persistence.foundationdb.util

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source
import com.apple.foundationdb.async.{AsyncIterable, AsyncIterator}
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.{Database, FDBException, KeySelector, KeyValue, ReadTransaction, ReadTransactionContext, StreamingMode, Transaction, TransactionContext, Range => FdbRange}

import scala.compat.java8.FutureConverters._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

object RangeRead {

  //todo make it lazy
  def longRunningRangeSource(range: FdbRange, limit: Option[Int])(implicit tcx: Database, ec: ExecutionContext): Source[KeyValue, NotUsed] = {
    longRunningRangeSource(range.begin, range.end, limit)
  }

  /**
    * Wrapper around [[rangeSource]] which can be used for doing long-running (more than 5 seconds) non-transactional queries.
    * @param from inclusive
    * @param to exclusive
    * @param limit None - no limit
    * @param db database object
    * @param ec
    * @return
    */
  def longRunningRangeSource(from: Array[Byte], to: Array[Byte], limit: Option[Int])(implicit db: Database, ec: ExecutionContext): Source[KeyValue, NotUsed] = {

    @volatile var currentPosition: Array[Byte] = from

    val validatedLimit = limit match {
      case None => ReadTransaction.ROW_LIMIT_UNLIMITED
      case Some(n) if n < 0 => throw new IllegalArgumentException("row limit can't be negative")
      case Some(n) => n
    }

    val elementsLeft: AtomicInteger = new AtomicInteger(validatedLimit)

    def getLimit(): Int = {
      limit match {
        case None => ReadTransaction.ROW_LIMIT_UNLIMITED
        case _ => elementsLeft.get()
      }
    }

    def recursiveRangeSource: Source[KeyValue, NotUsed] = {
      rangeSource(
        begin = KeySelector.firstGreaterOrEqual(from),
        end = KeySelector.firstGreaterOrEqual(to),
        limit = getLimit(),
        mode = StreamingMode.WANT_ALL
      )
      .map { kv =>
        currentPosition = Tuple.fromBytes(kv.getKey).range().begin
        elementsLeft.decrementAndGet()
        kv
      }
      .recoverWithRetries(-1, {
        case ex: FDBException if ex.getCode == 1007 =>
          recursiveRangeSource
      })
    }

    limit match {
      case Some(0) =>
        Source.empty
      case _ =>
        recursiveRangeSource
    }
  }

  def rangeSource(range: FdbRange,
                  limit: Int,
                  reverse: Boolean,
                  mode: StreamingMode)
                 (implicit tcx: ReadTransactionContext, ec: ExecutionContext): Source[KeyValue, NotUsed] = {
    asyncIterableToSource(_.getRange(range, limit, reverse, mode))
  }

  /**
    * Wrapper around [[ReadTransaction#getRange]] that represents [[com.apple.foundationdb.async.AsyncIterable]] as a [[Source]].
    *
    * @param begin the beginning of the range (inclusive)
    * @param end the end of the range (exclusive)
    * @param limit the maximum number of results to return. Limits results to the first keys in the range.
    *              Pass ROW_LIMIT_UNLIMITED if this query should not limit the number of results.
    *              If reverse is true rows will be limited starting at the end of the range.
    * @param reverse return results starting at the end of the range in reverse order
    * @param mode provide a hint about how the results are to be used. This can provide speed improvements or
    *             efficiency gains based on the caller's knowledge of the upcoming access pattern.
    * @param tcx transaction context
    * @param ec execution context
    * @return [[Source]] with the elements of the range
    */
  def rangeSource(begin: KeySelector,
                  end: KeySelector,
                  limit: Int = ReadTransaction.ROW_LIMIT_UNLIMITED,
                  reverse: Boolean = false,
                  mode: StreamingMode = StreamingMode.ITERATOR)
                 (implicit tcx: ReadTransactionContext, ec: ExecutionContext): Source[KeyValue, NotUsed] = {
    asyncIterableToSource(_.getRange(begin, end, limit, reverse, mode))

//    Source.unfoldResourceAsync[KeyValue, (AsyncIterator[KeyValue], Promise[NotUsed])](
//      () => {
//        val transactionPromise = Promise[ReadTransaction]()
//        val resultPromise = Promise[NotUsed]()
//        tcx.readAsync { tr =>
//          transactionPromise.trySuccess(tr)
//          resultPromise.future.toJava.toCompletableFuture
//        }
//        transactionPromise.future.map {
//          tr =>
//            val res = tr.getRange(begin, end, limit, reverse, mode)
//            res.iterator() -> resultPromise
//        }
//      },
//      {
//        case (iterator, resultPromise) => iterator.onHasNext().toScala.map {
//          case java.lang.Boolean.TRUE =>
//            Some(iterator.next())
//          case _ =>
//            resultPromise.trySuccess(NotUsed)
//            None
//        }
//      },
//      {
//        case (iterator, resultPromise) =>
//          Future.successful {
//            iterator.cancel()
//            resultPromise.trySuccess(NotUsed)
//            Done
//          }
//      }
//    )
  }

  private def asyncIterableToSource(getRange: ReadTransaction => AsyncIterable[KeyValue])(implicit tcx: ReadTransactionContext, ec: ExecutionContext): Source[KeyValue, NotUsed] = {
    Source.unfoldResourceAsync[KeyValue, (AsyncIterator[KeyValue], Promise[NotUsed])](
      () => {
        val transactionPromise = Promise[ReadTransaction]()
        val resultPromise = Promise[NotUsed]()
        tcx.readAsync { tr =>
          transactionPromise.trySuccess(tr)
          resultPromise.future.toJava.toCompletableFuture
        }
        transactionPromise.future.map {
          tr =>
            val res = getRange(tr)
            res.iterator() -> resultPromise
        }
      },
      {
        case (iterator, resultPromise) => iterator.onHasNext().toScala.map {
          case java.lang.Boolean.TRUE =>
            Some(iterator.next())
          case _ =>
            resultPromise.trySuccess(NotUsed)
            None
        }
      },
      {
        case (iterator, resultPromise) =>
          Future.successful {
            iterator.cancel()
            resultPromise.trySuccess(NotUsed)
            Done
          }
      }
    )
  }
}
