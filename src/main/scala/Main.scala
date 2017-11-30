package smallstm.Main

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference, LongAdder}
import java.util.concurrent.locks.ReentrantLock

import smallstm.Main.Common.{Block, RefSortKey, Value, Version}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Random, Success, Try}

// We exclude the case in which refs are created when running the
// futures (register cannot be called when runTransaction is)
object Common {
  type Block[T] = Unit => T
  type Version = Long
  type Value = Int
  type RefSortKey = Int

  private[smallstm] val rnd= new Random()
  rnd.setSeed(System.currentTimeMillis())
}

object Engine {

  // a global version number
  private[smallstm] val transactionNumber = new AtomicLong()

  def runTransaction[T](t: Block[T]) : T = {

    var done = false
    var result: Option[T] = None
    var LeftRetries = 10

    while (!done && LeftRetries > 0){
      val transaction = Transaction(t, transactionNumber.get())

//      Try {
//        transaction.run()
//      } catch (RollbackException)

      transaction.commit() match {
        case TransactionRollbacked => LeftRetries -= 1
        case TransactionCommited => done = true
      }

    }

    result.get
  }
}

class Ref private(initialValue: Int) {
  val id: RefSortKey= Common.rnd.nextInt()

  // one lock per ref
  val writeLock = new ReentrantLock()
  // the actual value, protected by the writeLock
  var value: (Int, Version) = (initialValue, 0)

  def set(value:Int)(implicit transactionExecutor: Transaction[_]) : Unit = {
    transactionExecutor.write(this, value)
  }

  def get()(implicit transactionExecutor: Transaction[_]) : Int = {
    // should rollback transaction if value is none
    // TODO: check status here & throw Rollback if needed
    transactionExecutor.read(this).get
  }
}

trait TransactionOutcome
case object TransactionCommited extends  TransactionOutcome
case object TransactionRollbacked extends TransactionOutcome

// suppose we get monothreaded transactions
case class Transaction[T](block: Block[T], initialVersion: Version) {
  val LockAcquireTimeoutMilliseconds = 10

  def commit(): TransactionOutcome = {
    val commitGlobalVersion = Engine.transactionNumber.longValue()

    // validate the read set
    val isReadSetValid = readSet.forall({
      case (ref, version) =>
        version <= commitGlobalVersion && !ref.writeLock.isLocked
    })

    // needs either List[Ref] \/ List[Ref]
    @tailrec def acquireLocks(locks: Seq[Ref], acquiredLocks: List[Ref]): (Boolean, List[Ref]) = locks match {
      case Nil => (true, acquiredLocks)
      case h::t => Try {
        h.writeLock.tryLock(LockAcquireTimeoutMilliseconds, TimeUnit.MILLISECONDS)
      }  match {
        case Success(true) => acquireLocks(t, h :: acquiredLocks)
        case _ => (false, acquiredLocks)
      }
    }

    // acquire locks on write ( maybe implement using a try.sequence )
    val (success, acquiredLocks) = acquireLocks(writeSet.keys.toList, List.empty)

    if (!success){
      // release all locks
      acquiredLocks.foreach(ref => ref.writeLock.unlock())
      TransactionRollbacked
    } else {
      // we got all locks
      // check again
      val writeVersion = Engine.transactionNumber.addAndGet(1)

      // validate the read set
      val isReadSetValid = readSet.forall({
        case (ref, version) => version <= commitGlobalVersion && !ref.writeLock.isLocked
      })

      if (!isReadSetValid) {
        acquiredLocks.foreach(ref => ref.writeLock.unlock())
        TransactionRollbacked
      } else {
        writeSet.foreach {
          case (ref, value) => ref.value = (value, writeVersion)
        }
        acquiredLocks.foreach(ref => ref.writeLock.unlock())
        TransactionCommited
      }
    }
  }

  // we could use a bloom filter here
  private[smallstm] val writeSet = mutable.Map[Ref, Value]()
  private[smallstm] val readSet = mutable.Map[Ref, Version]()
  def run(): T = block()

  // should not return Option,
  // should cancel the transaction "behind the scenes"
  def read(ref: Ref): Option[Value] = {
    readSet.get(ref)
      .filter(previouslyReadVersion => previouslyReadVersion != ref.value._2)
      .map(_ => {
        writeSet.get(ref) match {
          // read before write
          // could check that :
          // - if global has increased should we rollback ( independently from the ref) => NO ?
          // - if read Ref has increased globally we should
          //   fail the transaction (it has been read already, & version is <>)
          case None =>
            val (value, currentVersion) = ref.value
            readSet.put(ref, currentVersion)
            value
          // read after write case
          case Some(storedValue) => storedValue
        }
      })
  }

  def write(ref: Ref, value: Int): Unit = writeSet.put(ref, value)
}

object Ref {
  def apply(value: Int): Ref = new Ref(value)
}

class scala {
}
