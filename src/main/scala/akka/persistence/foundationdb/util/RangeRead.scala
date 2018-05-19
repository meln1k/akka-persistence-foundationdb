package akka.persistence.foundationdb.util

import java.util.concurrent.atomic.AtomicInteger

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source
import com.apple.foundationdb.async.AsyncIterator
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.{FDBException, KeySelector, KeyValue, StreamingMode, Transaction, TransactionContext, Range => FdbRange}

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

object RangeRead {

  def rangeSource(from: Array[Byte], to: Array[Byte], limit: Int)(implicit tcx: TransactionContext, ec: ExecutionContext): Source[KeyValue, NotUsed] = {

    @volatile var downloadState: QueryState = InProgress

    @volatile var start: Array[Byte] = from

    val validatedLimit = limit match {
      case n if n < 0 => 0
      case ok => ok
    }

    val elementsLeft: AtomicInteger = new AtomicInteger(validatedLimit)

    def rangeDownload = Source.unfoldResourceAsync[(KeyValue, Tuple), (AsyncIterator[KeyValue], Promise[NotUsed])](
      () => {
        val transactionPromise = Promise[Transaction]()
        val resultPromise = Promise[NotUsed]()
        tcx.runAsync { tr =>
          transactionPromise.trySuccess(tr)
          resultPromise.future.toJava.toCompletableFuture
        }
        transactionPromise.future.map {
          tr =>
            val res = tr.getRange(start, to, elementsLeft.get(), false, StreamingMode.WANT_ALL)
            res.iterator() -> resultPromise
        }
      },
      {
        case (iterator, resultPromise) => iterator.onHasNext().toScala.map {
          case t if t =>
            val next = iterator.next()
            val keyTuple = Tuple.fromBytes(next.getKey)
            Some(next -> keyTuple)
          case _ =>
            resultPromise.trySuccess(NotUsed)
            val newState = downloadState match {
              case InProgress => Finished
              case WasRestarted => InProgress
            }
            downloadState = newState
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


    def recursiveRangeDownload: Source[KeyValue, NotUsed] = {
      if (downloadState == Finished) {
        Source.empty
      } else {
        rangeDownload
          .map { case (kv, t) =>
            start = t.range().begin
            elementsLeft.decrementAndGet()
            kv
          }
          .recoverWithRetries(-1,{
            case ex: FDBException if ex.getCode == 1007 =>
              downloadState = WasRestarted
              recursiveRangeDownload
          })
      }
    }

    recursiveRangeDownload

  }

}


sealed trait QueryState
case object InProgress extends QueryState
case object WasRestarted extends QueryState
case object Finished extends QueryState
