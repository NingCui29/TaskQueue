package com.cn.tasks

import java.util.concurrent.CountDownLatch
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueProgressCallbackTest {
    @Test fun externalProgressCallbackRejectsSaturatedBlockReentryAndLetsAcceptedWorkFinish() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent)
            val release = CompletableDeferred<Unit>()
            val nestedResult = AtomicReference<EnqueueResult>()
            val convenienceFailure = AtomicReference<Throwable>()
            lateinit var reporter: ProgressReporter
            val first = Task(
                block = { reporter = it; release.await() },
                onStateChange = { state ->
                    if (state is TaskState.Running && state.progress == 0.5f) {
                        nestedResult.set(queue.enqueueWithResult(Task(block = { fail("Rejected work must not run") })))
                        convenienceFailure.set(runCatching {
                            queue.enqueue(Task(block = { fail("Rejected work must not run") }))
                        }.exceptionOrNull())
                    }
                },
            )
            val second = Task(block = { Unit })
            val outcome = FutureTask {
                reporter.report(0.5f)
                release.complete(Unit)
                Unit
            }
            val producer = Thread(outcome, "external-progress-reentry").apply { isDaemon = true }
            try {
                queue.enqueue(first)
                runCurrent()
                queue.enqueue(second)
                assertEquals(1, queue.pendingCount())

                producer.start()
                outcome.get(5, TimeUnit.SECONDS)

                assertCapacityRejection(nestedResult.get())
                assertTrue(convenienceFailure.get() is TaskQueueFullException)
                assertEquals(1, queue.pendingCount())
                runCurrent()

                assertEquals(TaskState.Completed(true), first.currentState)
                assertEquals(TaskState.Completed(true), second.currentState)
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                release.complete(Unit)
                queue.shutdown()
                owner.cancel()
                producer.interrupt()
                producer.join(5_000)
                runCurrent()
            }
        }
    }

    @Test fun progressNotificationRestoresTheExternalThreadsOrdinaryBlockAdmission() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent)
            val release = CompletableDeferred<Unit>()
            val reportReturned = CountDownLatch(1)
            val nestedResult = AtomicReference<EnqueueResult>()
            val ordinaryResult = AtomicReference<EnqueueResult>()
            lateinit var reporter: ProgressReporter
            val first = Task(
                block = { reporter = it; release.await() },
                onStateChange = { state ->
                    if (state is TaskState.Running && state.progress == 0.5f) {
                        nestedResult.set(queue.enqueueWithResult(Task(block = { fail("Rejected work must not run") })))
                    }
                },
            )
            val second = Task<Unit>(block = { fail("Waiting work must be cancelled to release capacity") })
            val ordinary = Task(block = { Unit })
            val outcome = FutureTask {
                reporter.report(0.5f)
                reportReturned.countDown()
                ordinaryResult.set(queue.enqueueWithResult(ordinary))
                release.complete(Unit)
                Unit
            }
            val producer = Thread(outcome, "external-progress-context-restoration").apply { isDaemon = true }
            try {
                queue.enqueue(first)
                runCurrent()
                queue.enqueue(second)

                producer.start()
                assertTrue(reportReturned.await(5, TimeUnit.SECONDS))
                awaitWaiting(producer, outcome)

                assertCapacityRejection(nestedResult.get())
                assertNull(ordinaryResult.get())
                assertFalse(outcome.isDone)
                assertEquals(1, queue.pendingCount())
                assertTrue(queue.cancel(second.id))
                outcome.get(5, TimeUnit.SECONDS)
                assertTrue(ordinaryResult.get() is EnqueueResult.Success)
                runCurrent()

                assertEquals(TaskState.Completed(true), first.currentState)
                assertEquals(TaskState.Cancelled, second.currentState)
                assertEquals(TaskState.Completed(true), ordinary.currentState)
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                release.complete(Unit)
                queue.shutdown()
                owner.cancel()
                producer.interrupt()
                producer.join(5_000)
                runCurrent()
            }
        }
    }

    @Test fun externalProgressCallbackFailurePropagatesAndRestoresItsQueueContext() = runTest {
        for (concurrent in listOf(false, true)) {
            for (failure in listOf(CancellationException("progress cancelled"), AssertionError("progress failed"))) {
                val dispatcher = StandardTestDispatcher(testScheduler)
                val owner = CoroutineScope(SupervisorJob() + dispatcher)
                val queue = queue(owner, dispatcher, concurrent)
                val release = CompletableDeferred<Unit>()
                lateinit var reporter: ProgressReporter
                lateinit var executionContext: QueueExecutionContext
                val task = Task(
                    block = {
                        reporter = it
                        executionContext = checkNotNull(currentCoroutineContext()[QueueExecutionContext])
                        release.await()
                    },
                    onStateChange = { state ->
                        if (state is TaskState.Running && state.progress == 0.5f) {
                            assertTrue(executionContext.isEntered)
                            throw failure
                        }
                    },
                )
                val outcome = FutureTask {
                    assertFalse(executionContext.isEntered)
                    assertSame(failure, runCatching { reporter.report(0.5f) }.exceptionOrNull())
                    assertFalse(executionContext.isEntered)
                    release.complete(Unit)
                    Unit
                }
                val producer = Thread(outcome, "external-progress-failure").apply { isDaemon = true }
                try {
                    queue.enqueue(task)
                    runCurrent()
                    producer.start()
                    outcome.get(5, TimeUnit.SECONDS)
                    runCurrent()

                    assertEquals(TaskState.Completed(true), task.currentState)
                    assertTrue(queue.isIdle())
                    assertTrue(owner.isActive)
                } finally {
                    release.complete(Unit)
                    queue.shutdown()
                    owner.cancel()
                    producer.interrupt()
                    producer.join(5_000)
                    runCurrent()
                }
            }
        }
    }

    private fun assertCapacityRejection(result: EnqueueResult?) {
        assertTrue("Saturated callback reentry must return a capacity rejection: $result", result is EnqueueResult.Rejected)
        assertEquals(EnqueueResult.RejectionCause.CAPACITY, (result as EnqueueResult.Rejected).cause)
    }

    private fun awaitWaiting(thread: Thread, outcome: FutureTask<*>) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        // Condition.await 允许伪唤醒，生产者可能在两次读取之间离开 WAITING；断言必须落在循环观察到的那一次读取上。
        var state = thread.state
        while (state != Thread.State.WAITING && !outcome.isDone && System.nanoTime() < deadline) {
            Thread.yield()
            state = thread.state
        }
        assertEquals("An ordinary producer must wait for capacity after the progress callback returns",
            Thread.State.WAITING, state)
    }

    private fun queue(owner: CoroutineScope, dispatcher: CoroutineDispatcher, concurrent: Boolean): QueueAccess =
        if (concurrent) {
            val queue = ConcurrentTaskQueue(owner, 1, dispatcher, BackpressureConfig.block(1))
            QueueAccess({ queue.enqueue(it); Unit }, queue::enqueueWithResult, queue::cancel,
                { queue.pendingTaskCount }, { queue.isIdle }, queue::shutdown)
        } else {
            val queue = SerialTaskQueue(owner, dispatcher, BackpressureConfig.block(1))
            QueueAccess({ queue.enqueue(it); Unit }, queue::enqueueWithResult, queue::cancel,
                { queue.pendingCount }, { queue.isIdle }, queue::shutdown)
        }

    private class QueueAccess(
        val enqueue: (Task<Unit>) -> Unit,
        val enqueueWithResult: (Task<Unit>) -> EnqueueResult,
        val cancel: (String) -> Boolean,
        val pendingCount: () -> Int,
        val isIdle: () -> Boolean,
        val shutdown: () -> Unit,
    )
}
