package smallstm

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference, LongAdder}
import java.util.concurrent.locks.ReentrantLock

import smallstm.Common.{Block, RefSortKey, Value, Version}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Random, Success, Try}

enum TransactionOutcome {
  case TransactionCommited
  case TransactionRollbacked
}

// We exclude the case in which refs are created when running the
// futures (register cannot be called when runTransaction is)
object Common {
  type Block[T] = Transaction[_] => T
  type Version = Long
  type Value = Int
  type RefSortKey = Int

  private[smallstm] val rnd = new Random()
  rnd.setSeed(System.currentTimeMillis())
}

class RollbackException() extends Exception

object Engine {
  import TransactionOutcome._

  // a global version number
  private[smallstm] val globalVersionNumber = new AtomicLong()

  def runTransaction[T](t: Block[T]): T = {

    var done = false
    var result: Option[T] = None
    var LeftRetries = 10000

    while (!done && LeftRetries > 0) {
      val transaction = Transaction(t)

      val transactionOutcome = try {
        result = Some(transaction.run(transaction))
        transaction.commit()
      } catch {
        case (e: RollbackException) => {
          TransactionRollbacked
        }
      }

      transactionOutcome match {
        case TransactionRollbacked => {
          LeftRetries -= 1
        }
        case TransactionCommited => {
          done = true
        }
      }

    }

    if (LeftRetries == 0) {
      throw new RuntimeException("could not commit")
    }

    result.get
  }
}

class Ref private (initialValue: Int) {
  val id: RefSortKey = Common.rnd.nextInt()

  // one lock per ref
  val writeLock = new ReentrantLock()
  // the actual value, protected by the writeLock
  @volatile var value: (Int, Version) = (initialValue, 0)

  def set(value: Int)(implicit transactionExecutor: Transaction[_]): Unit = {
    transactionExecutor.write(this, value)
  }

  def get()(implicit transactionExecutor: Transaction[_]): Int = {
    // should rollback transaction if value is none
    // TODO: check status here & throw Rollback if needed
    transactionExecutor.read(this) match {
      case Some(x) => x
      case None => {
        throw new RollbackException()
      }
    }
  }
}

// suppose we get monothreaded transactions
case class Transaction[T](block: Block[T]) {
  import TransactionOutcome._

  val LockAcquireTimeoutMilliseconds = 500L
  val readVersion = Engine.globalVersionNumber.get()

  def commit(): TransactionOutcome = {
    // validate the read set
    val isReadSetValid = readSet.forall({
      case ref =>
        ref.value._2 <= readVersion && !ref.writeLock.isLocked
    })

    // needs either List[Ref] \/ List[Ref]
    @tailrec def acquireLocks(locks: Seq[Ref],
                              acquiredLocks: List[Ref]): (Boolean, List[Ref]) =
      locks match {
        case Nil => (true, acquiredLocks)
        case h :: t =>
          Try {
            h.writeLock.tryLock(LockAcquireTimeoutMilliseconds,
                                TimeUnit.MILLISECONDS)
          } match {
            case Success(true) => acquireLocks(t, h :: acquiredLocks)
            case _             => (false, acquiredLocks)
          }
      }

    // acquire locks on write ( maybe implement using a try.sequence )
    val (success, acquiredLocks) =
      acquireLocks(writeSet.keys.toList, List.empty)

    if (!success) {
      // release all locks
      acquiredLocks.foreach(ref => ref.writeLock.unlock())

      TransactionRollbacked
    } else {
      // we got all locks, check readset again
      val writeVersion = Engine.globalVersionNumber.addAndGet(1)

      val acquiredLocksSet = acquiredLocks.toSet

      val isReadSetValid = readSet.exists(
        ref =>
          ref.value._2 > readVersion ||
            (!acquiredLocksSet.contains(ref) && ref.writeLock.isLocked))

      if (isReadSetValid) {
        acquiredLocks.foreach(ref => ref.writeLock.unlock())
        println(s"${Thread.currentThread.getName} rollbacked")
        TransactionRollbacked
      } else {
        writeSet.foreach {
          case (ref, value) => {
            ref.value = (value, writeVersion)
          }
        }
        acquiredLocks.foreach(ref => ref.writeLock.unlock())
        println(s"${Thread.currentThread.getName} commited")
        TransactionCommited
      }
    }
  }

  // we could use a bloom filter here
  private[smallstm] val writeSet = mutable.Map[Ref, Value]()
  private[smallstm] val readSet = mutable.Set[Ref]()
  def run(t: Transaction[_]): T = block(t)

  // should not return Option,
  // should cancel the transaction "behind the scenes"
  def read(ref: Ref): Option[Value] = {
    if (ref.value._2 > readVersion || ref.writeLock.isLocked) {
      None
    } else {
      writeSet.get(ref) match {
        // read before write
        // could check that :
        // - if global has increased should we rollback ( independently from the ref) => NO ?
        // - if read Ref has increased globally we should
        //   fail the transaction (it has been read already, & version is <>)
        case None =>
          val (value, _) = ref.value
          readSet += ref
          Some(value)
        // read after write case
        case someStoredValue => {
          someStoredValue
        }
      }
    }
  }

  def write(ref: Ref, value: Int): Unit = writeSet.put(ref, value)
}

object Ref {
  def apply(value: Int): Ref = new Ref(value)
}

object atomic {
  def apply[T](thunk: Transaction[_] => T) = Engine.runTransaction(thunk)
}

object Main {
  import scala.concurrent.Future
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.concurrent._

  def main(args: Array[String]): Unit = {
    val b = Ref(0)
    val a = Ref(100)

    val tasks = (1 to 100).map(i => {
      val c = new java.lang.Thread {
        override def run(): Unit = {
          atomic { implicit txn =>
            val from = b.get()
            val to = a.get()

            b.set(from + 1)
            a.set(to - 1)
          }
        }
      }
      c.setName(s"worker$i")
      c.start()
      c
    })
    tasks.foreach(t => t.join())

    println(atomic { implicit txn =>
      b.get().toString -> a.get().toString
    })
  }
}
