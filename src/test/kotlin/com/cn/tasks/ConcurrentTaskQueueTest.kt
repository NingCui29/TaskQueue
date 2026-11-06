package com.cn.tasks

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class ConcurrentTaskQueueTest {
    @Test fun individualCancellationAndCompletionObserveTaskDuringDispatchHandoff() = runTest {
        val queue = ConcurrentTaskQueue(backgroundScope, 1, StandardTestDispatcher(testScheduler))
        var cancelOnPositionUpdate = false
        var completionObserved = false
        val first = Task<Unit>(block = { fail("Polled task must still be found by cancel(id)") })
        lateinit var second: Task<Unit>
        second = Task(
            block = { fail("Pending task must be cancelled") },
            onStateChange = { state ->
                if (cancelOnPositionUpdate && state is TaskState.Pending && state.queueSize == 1) {
                    cancelOnPositionUpdate = false
                    assertTrue(queue.cancel(first.id))
                    assertTrue(queue.cancel(second.id))
                    assertFalse(queue.isIdle)
                    async(start = CoroutineStart.UNDISPATCHED) {
                        queue.awaitCompletion()
                        completionObserved = true
                    }
                    assertFalse(completionObserved)
                }
            },
        )
        queue.enqueue(first)
        queue.enqueue(second)
        cancelOnPositionUpdate = true

        runCurrent()

        assertEquals(TaskState.Cancelled, first.currentState)
        assertEquals(TaskState.Cancelled, second.currentState)
        assertTrue(completionObserved)
        assertTrue(queue.isIdle)
        assertTrue(backgroundScope.isActive)
    }

    @Test fun cancellationFromPendingCallbackAlsoCancelsTaskAlreadyPolled() = runTest {
        val queue = ConcurrentTaskQueue(backgroundScope, 1, StandardTestDispatcher(testScheduler))
        var cancelOnPositionUpdate = false
        var newTaskCompleted = false
        val first = Task<Unit>(block = { fail("Already polled task must also be cancelled") })
        val second = Task<Unit>(
            block = { fail("Pending task must be cancelled") },
            onStateChange = { state ->
                if (cancelOnPositionUpdate && state is TaskState.Pending && state.queueSize == 2) {
                    cancelOnPositionUpdate = false
                    queue.cancelAll()
                    queue.enqueue(Task(block = { newTaskCompleted = true }))
                }
            },
        )
        val third = Task<Unit>(block = { fail("Stale position snapshot must not restore cancelled task") })
        queue.enqueue(first)
        queue.enqueue(second)
        queue.enqueue(third)
        cancelOnPositionUpdate = true

        runCurrent()

        assertEquals(TaskState.Cancelled, first.currentState)
        assertEquals(TaskState.Cancelled, second.currentState)
        assertEquals(TaskState.Cancelled, third.currentState)
        assertTrue(newTaskCompleted)
        assertTrue(queue.isIdle)
        assertTrue(backgroundScope.isActive)
    }

    @Test fun cancellationBeforeTaskBodyStartsReturnsItsPermit() = runTest {
        val taskDispatcher = PausedDispatcher()
        val queue = ConcurrentTaskQueue(backgroundScope, maxConcurrency = 1, dispatcher = taskDispatcher)
        val first = Task<Unit>(block = { fail("Cancelled task body must not start") })
        var secondCompleted = false

        queue.enqueue(first)
        runCurrent() // Reserve the only permit and dispatch first, without running its body.
        assertEquals(1, queue.runningTaskCount)
        assertTrue(queue.cancel(first.id))
        queue.cancelAll() // Repeated cancellation must still return exactly one permit.
        taskDispatcher.runAll()
        runCurrent()

        assertEquals(TaskState.Cancelled, first.currentState)
        assertEquals(0, queue.runningTaskCount)
        queue.enqueue(Task(block = { secondCompleted = true }))
        runCurrent()
        taskDispatcher.runAll()
        runCurrent()

        assertTrue(secondCompleted)
        assertTrue(queue.isIdle)
        assertTrue(backgroundScope.isActive)
    }

    @Test fun shutdownRejectsProducerAlreadyBlockedOnCapacity() {
        val dispatcher = PausedDispatcher()
        val owner = CoroutineScope(SupervisorJob() + dispatcher)
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher, BackpressureConfig.block(1))
        val first = Task<Unit>(block = { fail("Pending task must be cancelled by shutdown") })
        val blocked = Task<Unit>(block = { fail("Shutdown must reject the blocked producer") })
        queue.enqueue(first) // Keep the dispatch loop parked so the pending queue remains full.
        val result = FutureTask { queue.enqueueWithResult(blocked) }
        val producer = Thread(result, "blocked-task-producer")
        try {
            producer.start()
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
            // Condition.await 允许伪唤醒，生产者可能在两次读取之间离开 WAITING；断言必须落在循环观察到的那一次读取上。
            var producerState = producer.state
            while (producerState != Thread.State.WAITING && System.nanoTime() < deadline) {
                Thread.yield()
                producerState = producer.state
            }
            assertEquals(Thread.State.WAITING, producerState)
            queue.shutdown()

            assertTrue(result.get(5, TimeUnit.SECONDS) is EnqueueResult.Rejected)
            assertEquals(TaskState.Cancelled, blocked.currentState)
            assertEquals(0, queue.pendingTaskCount)
            assertTrue(owner.isActive)
        } finally {
            queue.shutdown()
            producer.interrupt()
            producer.join(5_000)
            owner.cancel()
            dispatcher.runAll()
        }
    }

    private class PausedDispatcher : CoroutineDispatcher() {
        private val work = ConcurrentLinkedQueue<Runnable>()

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            work.add(block)
        }

        fun runAll() {
            while (true) (work.poll() ?: return).run()
        }
    }
}
