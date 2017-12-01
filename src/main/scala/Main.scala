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

class RollbackException() extends Exception

object Engine {

  // a global version number
  private[smallstm] val transactionNumber = new AtomicLong()

  def runTransaction[T](t: Block[T]) : T = {

    var done = false
    var result: Option[T] = None
    var LeftRetries = 10

    while (!done && LeftRetries > 0){
      val transaction = Transaction(t, transactionNumber.get())

      val transactionOutcome = try {
        println("running transaction")
        result = Some(transaction.run(transaction))
        println("committing transaction")
        transaction.commit()
      } catch {
        case (e:RollbackException) => {
          println("rollback during execution")
          TransactionRollbacked
        }
      }

      transactionOutcome match {
        case TransactionRollbacked => {
          println("lol")
          LeftRetries -= 1
        }
        case TransactionCommited => {
          println("COMMIT")
          done = true
        }
      }

    }

    if (LeftRetries  == 0) throw new RuntimeException("could not commit")

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
    transactionExecutor.read(this) match {
      case Some(x) => x
      case None => {
        println("** invalid ref value while reading")
        throw new RollbackException()
      }
    }
  }
}

trait TransactionOutcome
case object TransactionCommited extends  TransactionOutcome
case object TransactionRollbacked extends TransactionOutcome

// suppose we get monothreaded transactions
case class Transaction[T](block: Block[T], initialVersion: Version) {
  val LockAcquireTimeoutMilliseconds = 10
  val transactionGlobalVersion = Engine.transactionNumber.longValue()

  def commit(): TransactionOutcome = {
    val commitGlobalVersion = Engine.transactionNumber.longValue()

    // validate the read set
    val isReadSetValid = readSet.forall({
      case ref =>
        ref.value._2 <= commitGlobalVersion && !ref.writeLock.isLocked
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

    println("acquiring locks")
    // acquire locks on write ( maybe implement using a try.sequence )
    val (success, acquiredLocks) = acquireLocks(writeSet.keys.toList, List.empty)

    if (!success){
      println("could not get write locks, ROLLBACK")
      // release all locks
      acquiredLocks.foreach(ref => ref.writeLock.unlock())
      TransactionRollbacked
    } else {
      println("acquired locks")
      // we got all locks
      // check again
      val writeVersion = Engine.transactionNumber.addAndGet(1)

      // validate the read set
      val isReadSetValid = readSet.forall({
        case ref => ref.value._2 <= commitGlobalVersion && !ref.writeLock.isLocked
      })

      if (!isReadSetValid) {
        println("readSet is invalid, ROLLBACK")
        acquiredLocks.foreach(ref => ref.writeLock.unlock())
        TransactionRollbacked
      } else {
        println("readSet is valid, updating values")
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
  private[smallstm] val readSet = mutable.Set[Ref]()
  def run(t: Transaction[_]): T = block(t)

  // should not return Option,
  // should cancel the transaction "behind the scenes"
  def read(ref: Ref): Option[Value] = {
      if (ref.value._2 > transactionGlobalVersion) {
        println("Ref has expired")
        None
      } else {
        writeSet.get(ref) match {
          // read before write
          // could check that :
          // - if global has increased should we rollback ( independently from the ref) => NO ?
          // - if read Ref has increased globally we should
          //   fail the transaction (it has been read already, & version is <>)
          case None =>
            println("Ref is read for the first time")
            val (value, _) = ref.value
            readSet += ref
            Some(value)
          // read after write case
          case someStoredValue => {
            println("Ref is read from writeSet")
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

object Main extends App {
  import scala.concurrent.Future
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.concurrent._

  val a = Ref(100)
  val b = Ref(0)

  val sum: Seq[Future[Int]] = (1 to 10).map(i =>
    Future {
      atomic {
        implicit executor =>
          println("i : " + i)
          //a.set(a() - 1 )
          b.set(b.get() + 1)
          b.get()
      }
    }.map(s => i -> s))
    .map(f => f.map({
      case (i, s) =>
        println(s"resultl $i : $s")
        s
    }))


  Await.result(Future.sequence(sum), 10.seconds)
}