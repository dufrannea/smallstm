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
  type Block[T] = Transaction[_] => T
  type Version = Long
  type Value = Int
  type RefSortKey = Int

  private[smallstm] val rnd= new Random()
  rnd.setSeed(System.currentTimeMillis())
}

object Logger {
  var messages: scala.collection.mutable.ListBuffer[String] = new scala.collection.mutable.ListBuffer[String]()
  def debug(value: String): Unit = {
    this.synchronized {
      val id = java.lang.Thread.currentThread.getName()
      messages += (id + " : " + value)  
    }
  }

  def print = {
    messages.foreach(x => println(x))
  }
}

class RollbackException() extends Exception

object Engine {

  // a global version number
  private[smallstm] val globalVersionNumber = new AtomicLong()

  def runTransaction[T](t: Block[T]): T = {

    var done = false
    var result: Option[T] = None
    var LeftRetries = 1000

    while (!done && LeftRetries > 0){
      val transaction = Transaction(t)

      val transactionOutcome = try {
        Logger.debug("running transaction")
        result = Some(transaction.run(transaction))
        Logger.debug("committing transaction")
        transaction.commit()
      } catch {
        case (e:RollbackException) => {
          Logger.debug("rollback during execution")
          TransactionRollbacked
        }
      }

      transactionOutcome match {
        case TransactionRollbacked => {
          LeftRetries -= 1
          Logger.debug(s"transaction outcome is Rollback, $LeftRetries tries left")
        }
        case TransactionCommited => {
          Logger.debug("COMMIT")
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

class Ref private(initialValue: Int) {
  val id: RefSortKey= Common.rnd.nextInt()

  // one lock per ref
  val writeLock = new ReentrantLock()
  // the actual value, protected by the writeLock
  @volatile var value: (Int, Version) = (initialValue, 0)

  def set(value:Int)(implicit transactionExecutor: Transaction[_]) : Unit = {
    transactionExecutor.write(this, value)
  }

  def get()(implicit transactionExecutor: Transaction[_]) : Int = {
    // should rollback transaction if value is none
    // TODO: check status here & throw Rollback if needed
    transactionExecutor.read(this) match {
      case Some(x) => x
      case None => {
        Logger.debug("** invalid ref value while reading")
        throw new RollbackException()
      }
    }
  }
}

trait TransactionOutcome
case object TransactionCommited extends  TransactionOutcome
case object TransactionRollbacked extends TransactionOutcome

// suppose we get monothreaded transactions
case class Transaction[T](block: Block[T]) {
  val LockAcquireTimeoutMilliseconds = 100
  val readVersion = Engine.globalVersionNumber.get()

  Logger.debug(s"readVersion is $readVersion")
  def commit(): TransactionOutcome = {
    // validate the read set
    val isReadSetValid = readSet.forall({
      case ref =>
        ref.value._2 <= readVersion && !ref.writeLock.isLocked
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

    Logger.debug("acquiring locks")
    // acquire locks on write ( maybe implement using a try.sequence )
    val (success, acquiredLocks) = acquireLocks(writeSet.keys.toList, List.empty)

    if (!success){
      Logger.debug("could not get write locks, ROLLBACK")
      // release all locks
      acquiredLocks.foreach(ref => ref.writeLock.unlock())
      Logger.debug("locks released")

      TransactionRollbacked
    } else {
      Logger.debug(s"acquired locks ${acquiredLocks.size}")
      // we got all locks
      // check again
      val writeVersion = Engine.globalVersionNumber.addAndGet(1)
      Logger.debug(s"Incremented global version number to $writeVersion")
      enum InvalidReason {
        case Locked 
        case OldVersion(refVersion: Version, commitVersion: Version)
      }
      
      // validate the read set
      val invalidReason = readSet
        .collectFirst[InvalidReason]({
          case ref if ref.value._2 > readVersion => InvalidReason.OldVersion(ref.value._2, readVersion)
          case ref if !(acquiredLocks.toSet.contains(ref)) && ref.writeLock.isLocked => InvalidReason.Locked
        })

      invalidReason match {
        case Some(reason) => 
          Logger.debug(s"readSet is invalid, ROLLBACK ($reason)")
          acquiredLocks.foreach(ref => ref.writeLock.unlock())
          Logger.debug("locks released")
          TransactionRollbacked
        case _ => 
          Logger.debug(s"readSet is valid, updating values (to $writeVersion)")
          writeSet.foreach {
            case (ref, value) => {
              Logger.debug(s"wrote $value")
              ref.value = (value, writeVersion)
            }
          }
          Logger.debug("locks released")
          acquiredLocks.foreach(ref => ref.writeLock.unlock())
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
      if (ref.value._2 > readVersion) {
        Logger.debug(s"Error, ref updated (version ${ref.value._2} vs $readVersion)")
        None
      } else if ( ref.writeLock.isLocked){
          Logger.debug(s"Error, ref is locked")
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
            Logger.debug(s"Ref is read for the first time ($value)")
            readSet += ref
            Some(value)
          // read after write case
          case someStoredValue => {
            Logger.debug(s"Ref is read from writeSet ${someStoredValue.get}")
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
  def apply[T](thunk : Transaction[_] => T) = Engine.runTransaction(thunk)
}

object Main {
  import scala.concurrent.Future
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.concurrent._

  def main(args: Array[String]): Unit = {
    val b = Ref(0)

    // val sum: Seq[Future[Int]] = (1 to 10).map(i =>
    //   Future {
    //     atomic {
    //       implicit executor =>
    //         Logger.debug("i : " + i)
    //         //a.set(a() - 1 )
    //         b.set(b.get() + 1)
    //         b.get()
    //     }
    //   }.map(s => i -> s))
    //   .map(f => f.map({
    //     case (i, s) =>
    //       Logger.debug(s"resultl $i : $s")
    //       s
    //   }))

    // Await.result(Future.sequence(sum), 10.seconds)

   val tasks = (1 to 100).map(i => {
      val c = new java.lang.Thread {
        override def run(): Unit = {
           atomic {
              implicit txn =>
                val from = b.get()
                b.set(from + 1)
            }
        }
      }
      c.setName(s"worker$i")
      c.start()
      c
    })
    tasks.foreach(t => t.join())
    // increase b
    // (1 to 10).foreach(_ => {
      // Future {
      //   atomic {
      //     implicit txn =>
      //       val from = b.get()
      //       b.set(from + 1)
      //   }
      // }
    // })

    // display b's value
    Logger.debug(atomic {
      implicit txn =>
        b.get().toString
    })

    Logger.print
  }
}