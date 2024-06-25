package org.apache.gluten.utils.iterator

import org.apache.gluten.utils.iterator.Iterators.WrapperBuilder

import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.util.TaskResources

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

object IteratorsV1 {
  private class PayloadCloser[A](in: Iterator[A])(closeCallback: A => Unit) extends Iterator[A] {
    private var closer: Option[() => Unit] = None

    TaskResources.addRecycler("Iterators#PayloadCloser", 100) {
      tryClose()
    }

    override def hasNext: Boolean = {
      tryClose()
      in.hasNext
    }

    override def next(): A = {
      val a: A = in.next()
      closer.synchronized {
        closer = Some(
          () => {
            closeCallback.apply(a)
          })
      }
      a
    }

    private def tryClose(): Unit = {
      closer.synchronized {
        closer match {
          case Some(c) => c.apply()
          case None =>
        }
        closer = None // make sure the payload is closed once
      }
    }
  }

  private class IteratorCompleter[A](in: Iterator[A])(completionCallback: => Unit)
    extends Iterator[A] {
    private val completed = new AtomicBoolean(false)

    TaskResources.addRecycler("Iterators#IteratorRecycler", 100) {
      tryComplete()
    }

    override def hasNext: Boolean = {
      val out = in.hasNext
      if (!out) {
        tryComplete()
      }
      out
    }

    override def next(): A = {
      in.next()
    }

    private def tryComplete(): Unit = {
      if (!completed.compareAndSet(false, true)) {
        return // make sure the iterator is completed once
      }
      completionCallback
    }
  }

  private class LifeTimeAccumulator[A](in: Iterator[A], onCollected: Long => Unit)
    extends Iterator[A] {
    private val closed = new AtomicBoolean(false)
    private val startTime = System.nanoTime()

    TaskResources.addRecycler("Iterators#LifeTimeAccumulator", 100) {
      tryFinish()
    }

    override def hasNext: Boolean = {
      val out = in.hasNext
      if (!out) {
        tryFinish()
      }
      out
    }

    override def next(): A = {
      in.next()
    }

    private def tryFinish(): Unit = {
      // pipeline metric should only be calculate once.
      if (!closed.compareAndSet(false, true)) {
        return
      }
      val lifeTime = TimeUnit.NANOSECONDS.toMillis(
        System.nanoTime() - startTime
      )
      onCollected(lifeTime)
    }
  }

  private class ReadTimeAccumulator[A](in: Iterator[A], onAdded: Long => Unit) extends Iterator[A] {

    override def hasNext: Boolean = {
      val prev = System.nanoTime()
      val out = in.hasNext
      val after = System.nanoTime()
      val duration = TimeUnit.NANOSECONDS.toMillis(after - prev)
      onAdded(duration)
      out
    }

    override def next(): A = {
      val prev = System.nanoTime()
      val out = in.next()
      val after = System.nanoTime()
      val duration = TimeUnit.NANOSECONDS.toMillis(after - prev)
      onAdded(duration)
      out
    }
  }

  /**
   * To protect the wrapped iterator to avoid undesired order of calls to its `hasNext` and `next`
   * methods.
   */
  private class InvocationFlowProtection[A](in: Iterator[A]) extends Iterator[A] {
    sealed private trait State
    private case object Init extends State
    private case class HasNextCalled(hasNext: Boolean) extends State
    private case object NextCalled extends State

    private var state: State = Init

    override def hasNext: Boolean = {
      val out = state match {
        case Init | NextCalled =>
          in.hasNext
        case HasNextCalled(lastHasNext) =>
          lastHasNext
      }
      state = HasNextCalled(out)
      out
    }

    override def next(): A = {
      val out = state match {
        case Init | NextCalled =>
          if (!in.hasNext) {
            throw new IllegalStateException("End of stream")
          }
          in.next()
        case HasNextCalled(lastHasNext) =>
          if (!lastHasNext) {
            throw new IllegalStateException("End of stream")
          }
          in.next()
      }
      state = NextCalled
      out
    }
  }

  class WrapperBuilderV1[A] private[iterator] (in: Iterator[A]) extends WrapperBuilder[A] {
    private var wrapped: Iterator[A] = in

    override def recyclePayload(closeCallback: (A) => Unit): WrapperBuilder[A] = {
      wrapped = new PayloadCloser(wrapped)(closeCallback)
      this
    }

    override def recycleIterator(completionCallback: => Unit): WrapperBuilder[A] = {
      wrapped = new IteratorCompleter(wrapped)(completionCallback)
      this
    }

    override def collectLifeMillis(onCollected: Long => Unit): WrapperBuilder[A] = {
      wrapped = new LifeTimeAccumulator[A](wrapped, onCollected)
      this
    }

    override def collectReadMillis(onAdded: Long => Unit): WrapperBuilder[A] = {
      wrapped = new ReadTimeAccumulator[A](wrapped, onAdded)
      this
    }

    override def asInterruptible(context: TaskContext): WrapperBuilder[A] = {
      wrapped = new InterruptibleIterator[A](context, wrapped)
      this
    }

    override def protectInvocationFlow(): WrapperBuilder[A] = {
      wrapped = new InvocationFlowProtection[A](wrapped)
      this
    }

    override def create(): Iterator[A] = {
      wrapped
    }
  }
}
