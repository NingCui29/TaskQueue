package com.cn.tasks

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.*
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueShutdownTest {
    @Test fun explicitClosureRejectsBothApisWithoutCancellingOwner() = runTest {
        factories.forEach { factory ->
            var rejectedCount = 0
            val queue = factory(this, StandardTestDispatcher(testScheduler),
                BackpressureConfig(onRejected = { _, _ -> rejectedCount++ }))
            queue.shutdown()
            val task = Task<Unit>(block = { fail("Closed queue must not execute") })
            val result = queue.enqueue(task) as EnqueueResult.Rejected
            assertEquals(EnqueueResult.RejectionCause.CLOSED, result.cause)
            assertEquals(TaskState.Cancelled, task.currentState)
            assertEquals(1, rejectedCount)
            try {
                queue.submit(Task(block = { Unit }))
                fail("Convenience API must report closure explicitly")
            } catch (_: TaskQueueClosedException) { }
            assertTrue(queue.isTerminated())
            assertTrue(isActive)
        }
        listOf(TaskQueueScope.serial(this), TaskQueueScope.concurrent(this)).forEach { queue ->
            queue.shutdown()
            try {
                queue.enqueue(Task(block = { Unit }))
                fail("Closed queue scope must not accept tasks")
            } catch (_: TaskQueueClosedException) { }
            assertTrue(isActive)
        }
    }

    @Test fun ownerCancellationWakesBlockedProducerBeforeOwnerChildrenFinish() {
        factories.forEach { factory ->
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = factory(owner, dispatcher, BackpressureConfig.block(1))
            val pending = Task<Unit>(block = { fail("Owner cancellation must cancel pending work") })
            val blocked = Task<Unit>(block = { fail("Closed queue must reject blocked work") })
            queue.enqueue(pending)
            val result = FutureTask { queue.enqueue(blocked) }
            val producer = Thread(result, "shutdown-block-producer")
            try {
                producer.start()
                awaitWaiting(producer)
                owner.cancel()
                // The owner still has a cancelled dispatcher runnable. Rejection must not wait for it.
                val rejected = result.get(5, TimeUnit.SECONDS) as EnqueueResult.Rejected
                assertEquals(EnqueueResult.RejectionCause.CLOSED, rejected.cause)
                assertEquals(TaskState.Cancelled, pending.currentState)
                assertEquals(TaskState.Cancelled, blocked.currentState)
                val late = queue.enqueue(Task(block = { Unit })) as EnqueueResult.Rejected
                assertEquals(EnqueueResult.RejectionCause.CLOSED, late.cause)
                val createdAfterCancellation = factory(owner, dispatcher, BackpressureConfig.unbounded())
                val neverAdmitted = createdAfterCancellation.enqueue(Task(block = { Unit })) as EnqueueResult.Rejected
                assertEquals(EnqueueResult.RejectionCause.CLOSED, neverAdmitted.cause)
            } finally {
                queue.shutdown()
                owner.cancel()
                producer.interrupt()
                producer.join(5_000)
                dispatcher.runAll()
            }
        }
    }

    @Test fun cancelAllRemainsReusableForBothQueues() = runTest {
        factories.forEach { factory ->
            val queue = factory(this, StandardTestDispatcher(testScheduler), BackpressureConfig.unbounded())
            queue.enqueue(Task(block = { fail("cancelAll must cancel old pending work") }))
            queue.cancelAll()
            var completed = false
            queue.enqueue(Task(block = { completed = true }))
            runCurrent()
            assertTrue(completed)
            assertFalse(queue.isTerminated())
            assertTrue(queue.isIdle())
            queue.shutdown()
            assertTrue(isActive)
        }
    }

    @Test fun shutdownKeepsCleanupOwnedUntilTheTaskActuallyFinishes() = runTest {
        factories.forEach { factory ->
            val queue = factory(this, StandardTestDispatcher(testScheduler), BackpressureConfig.unbounded())
            val cleanup = CompletableDeferred<Unit>()
            queue.enqueue(Task<Unit>(block = {
                try { awaitCancellation() }
                finally { withContext(NonCancellable) { cleanup.await() } }
            }))
            runCurrent()
            try {
                queue.shutdown()
                runCurrent()
                assertTrue(queue.isClosed())
                assertFalse(queue.isTerminated())
                assertFalse(queue.isIdle())
                assertTrue(isActive)
            } finally { cleanup.complete(Unit) }
            runCurrent()
            assertTrue(queue.isIdle())
        }
    }

    @Test fun concurrentAwaitCompletionWaitsForShutdownCleanup() = runTest {
        val queue = ConcurrentTaskQueue(this, 1, StandardTestDispatcher(testScheduler))
        val cleanup = CompletableDeferred<Unit>()
        queue.enqueue(Task<Unit>(block = {
            try { awaitCancellation() }
            finally { withContext(NonCancellable) { cleanup.await() } }
        }))
        runCurrent()
        queue.shutdown()
        val completion = async { queue.awaitCompletion() }
        try {
            runCurrent()
            assertFalse(completion.isCompleted)
            assertTrue(queue.state.value is ConcurrentTaskQueue.ConcurrentState.Closing)
        } finally { cleanup.complete(Unit) }
        runCurrent()
        completion.await()
        assertEquals(ConcurrentTaskQueue.ConcurrentState.Shutdown, queue.state.value)
        assertTrue(isActive)
    }

    @Test fun callerRunsWorkAlsoBelongsToQueueShutdown() {
        factories.forEach { factory ->
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = factory(owner, dispatcher, BackpressureConfig.callerRuns(1))
            queue.enqueue(Task(block = { Unit }))
            val started = CountDownLatch(1)
            val cleaning = CountDownLatch(1)
            val release = CompletableDeferred<Unit>()
            val caller = Task<Unit>(block = {
                started.countDown()
                try { awaitCancellation() }
                finally {
                    cleaning.countDown()
                    withContext(NonCancellable) { release.await() }
                }
            })
            val result = FutureTask { queue.enqueue(caller) }
            val producer = Thread(result, "shutdown-caller-runs")
            try {
                producer.start()
                assertTrue(started.await(5, TimeUnit.SECONDS))
                queue.shutdown()
                assertTrue(cleaning.await(5, TimeUnit.SECONDS))
                assertFalse(queue.isIdle())
                assertTrue(owner.isActive)
                release.complete(Unit)
                assertFalse((result.get(5, TimeUnit.SECONDS) as EnqueueResult.CallerExecuted).success)
                dispatcher.runAll()
                assertEquals(TaskState.Cancelled, caller.currentState)
                assertTrue(queue.isIdle())
            } finally {
                release.complete(Unit)
                queue.shutdown()
                owner.cancel()
                producer.interrupt()
                producer.join(5_000)
                dispatcher.runAll()
            }
        }
    }

    private class QueueAccess(
        val enqueue: (Task<Unit>) -> EnqueueResult,
        val submit: (Task<Unit>) -> Task<Unit>,
        val shutdown: () -> Unit,
        val cancelAll: () -> Unit,
        val isIdle: () -> Boolean,
        val isClosed: () -> Boolean,
        val isTerminated: () -> Boolean,
    )

    private val factories: List<(CoroutineScope, CoroutineDispatcher, BackpressureConfig) -> QueueAccess> = listOf(
        { scope, dispatcher, config ->
            val queue = SerialTaskQueue(scope, dispatcher, config)
            QueueAccess(queue::enqueueWithResult, queue::enqueue, queue::shutdown, queue::cancelAll,
                { queue.isIdle }, { queue.isClosed }, { queue.isTerminated })
        },
        { scope, dispatcher, config ->
            val queue = ConcurrentTaskQueue(scope, 1, dispatcher, config)
            QueueAccess(queue::enqueueWithResult, queue::enqueue, queue::shutdown, queue::cancelAll,
                { queue.isIdle }, { queue.isClosed }, { queue.isTerminated })
        },
    )

    private fun awaitWaiting(thread: Thread) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (thread.state != Thread.State.WAITING && System.nanoTime() < deadline) Thread.yield()
        assertEquals(Thread.State.WAITING, thread.state)
    }

    private class PausedDispatcher : CoroutineDispatcher() {
        private val work = ConcurrentLinkedQueue<Runnable>()
        override fun dispatch(context: CoroutineContext, block: Runnable) { work.add(block) }
        fun runAll() { while (true) (work.poll() ?: return).run() }
    }
}
