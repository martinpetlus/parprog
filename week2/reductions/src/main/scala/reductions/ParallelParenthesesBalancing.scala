package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def helper(index: Int, leftCount: Int): Boolean = {
      if (index == chars.length) if (leftCount == 0) true else false
      else if (chars(index) == '(') helper(index + 1, leftCount + 1)
      else if (chars(index) == ')') if (leftCount > 0) helper(index + 1, leftCount - 1) else false
      else helper(index + 1, leftCount)
    }

    helper(0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int): (Int, Int) = {
      var i = idx
      var leftCount = 0
      var missingLeft = 0

      while (i < until) {
        if (chars(i) == ')') {
          if (leftCount > 0) leftCount = leftCount - 1
          else missingLeft = missingLeft + 1
        } else if (chars(i) == '(') {
          leftCount = leftCount + 1
        }

        i = i + 1
      }

      (missingLeft, leftCount)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold)
        traverse(from, until)
      else {
        val mid = (until - from) / 2 + from
        val ((missingLeft1, missingRight1), (missingLeft2, missingRight2)) = parallel(
          reduce(from, mid),
          reduce(mid, until)
        )
        val diff = missingLeft2 - missingRight1
        if (diff > 0) (missingLeft1 + diff, missingRight2)
        else (missingLeft1, missingRight2 + Math.abs(diff))
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
