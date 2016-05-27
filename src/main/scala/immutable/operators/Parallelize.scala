package immutable.operators

import immutable.{Column, Config, DataVector}
import scala.collection.{BitSet, mutable}
import scala.concurrent.{Future}
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by marcin on 5/22/16.
  *
  * 1. Register each chunk as separate column
  *     - will this consume more memory?
  * 2. Push argument down to encoder and attach encoder at scan time to column
  */
case class Parallelize(op: SelectionOperator, num: Int) extends SelectionOperator {
    def iterator = new ParallelSelectIterator()

    class ParallelSelectIterator extends Iterator[DataVector] {
        val q = new LinkedBlockingQueue[DataVector](1000)
        val opIters = for (i <- 0 until num) yield op.iterator
        var dudCounter = 0

        // Starting up thread for each input operator
        for (i <- 0 until num) {
            new Thread(new Runnable {
                def run() {
                    while (opIters(i).hasNext) {
                        q.put(opIters(i).next)
                    }
                    q.put(new DataVectorDud(0, List(), List[Array[Any]](), mutable.BitSet()))
                }
            }).start()
        }

        def next = {
            q.take
        }

        def hasNext = {
            q.peek match {
                case x: DataVectorDud => {
                    if (dudCounter + 1 == num) false
                    else {
                        dudCounter += 1
                        true
                    }
                }
                case x: DataVector => true
                case _ => Thread.sleep(1); hasNext
            }
        }
    }
}

object ParallelHelper {
    val registry = mutable.HashMap[String, (Int, Int)]()

    def add(colName: String) = {
        registry.get(colName) match {
            case Some(x) => registry.put(colName, (x._1 + 1, x._2 + 1))
            case None => registry.put(colName, (1, 0))
        }
    }

    def take(colName: String): Int = {
        registry.get(colName) match {
            case Some(x)  => registry.put(colName, (x._1, x._2 - 1)); x._2
            case None => 0
        }
    }
}

class DataVectorDud(vecID: Int, cols: List[Column], data: List[Array[_]], selected: mutable.BitSet) extends DataVector(vecID, cols, data, selected)
