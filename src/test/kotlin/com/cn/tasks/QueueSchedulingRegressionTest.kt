package com.cn.tasks

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueSchedulingRegressionTest {
    @Test fun initialPendingCallbackRejectsSaturatedBlockReentryWithoutStallingAcceptedWork() {
        for (concurrent in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.block(1))
            val attempted = AtomicBoolean(false)
            val nestedResult = AtomicReference<EnqueueResult>()
            val convenienceFailure = AtomicReference<Throwable>()
            val first = Task(block = { Unit }, onStateChange = { state ->
                if (state is TaskState.Pending && attempted.compareAndSet(false, true)) {
                    nestedResult.set(queue.enqueueWithResult(Task(block = { fail("Rejected task must not run") })))
                    convenienceFailure.set(runCatching {
                        queue.enqueue(Task(block = { fail("Rejected task must not run") }))
                    }.exceptionOrNull())
                }
            })
            try {
                onProducerThread(queue) { queue.enqueue(first) }
                assertCapacityRejection(nestedResult.get())
                assertTrue(convenienceFailure.get() is TaskQueueFullException)
                assertEquals(1, queue.pendingCount())
                dispatcher.runAll()
                assertEquals(TaskState.Completed(true), first.currentState)
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                queue.shutdown()
                owner.cancel()
                dispatcher.runAll()
            }
        }
    }

    @Test fun dispatchPendingCallbackRejectsSaturatedBlockReentryAndRunsAllAcceptedTasks() {
        for (concurrent in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.block(2))
            val armed = AtomicBoolean(false)
            val attempted = AtomicBoolean(false)
            val accepted = AtomicReference<EnqueueResult>()
            val rejected = AtomicReference<EnqueueResult>()
            val convenienceFailure = AtomicReference<Throwable>()
            val completed = mutableListOf<String>()
            val first = Task(block = { completed += "first" })
            val third = Task(block = { completed += "third" })
            val second = Task(block = { completed += "second" }, onStateChange = { state ->
                if (armed.get() && state is TaskState.Pending && state.queueSize == 1 &&
                    attempted.compareAndSet(false, true)
                ) {
                    accepted.set(queue.enqueueWithResult(third))
                    rejected.set(queue.enqueueWithResult(Task(block = { fail("Rejected task must not run") })))
                    convenienceFailure.set(runCatching {
                        queue.enqueue(Task(block = { fail("Rejected task must not run") }))
                    }.exceptionOrNull())
                }
            })
            try {
                queue.enqueue(first)
                queue.enqueue(second)
                armed.set(true)
                onProducerThread(queue, dispatcher::runAll)
                assertTrue(attempted.get())
                assertTrue(accepted.get() is EnqueueResult.Success)
                assertCapacityRejection(rejected.get())
                assertTrue(convenienceFailure.get() is TaskQueueFullException)
                assertEquals(listOf("first", "second", "third"), completed)
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                queue.shutdown()
                owner.cancel()
                dispatcher.runAll()
            }
        }
    }

    @Test fun thresholdCallbackRejectsSaturatedBlockReentry() {
        for (concurrent in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val attempted = AtomicBoolean(false)
            val nestedResult = AtomicReference<EnqueueResult>()
            lateinit var queue: QueueHarness
            val config = BackpressureConfig(
                maxCapacity = 1,
                policy = RejectionPolicy.BLOCK,
                onThresholdReached = { _, _, _ ->
                    if (attempted.compareAndSet(false, true)) {
                        nestedResult.set(queue.enqueueWithResult(Task(block = { fail("Rejected task must not run") })))
                    }
                },
            )
            queue = queue(owner, dispatcher, concurrent, config)
            val first = Task(block = { Unit })
            try {
                onProducerThread(queue) { queue.enqueue(first) }
                assertCapacityRejection(nestedResult.get())
                assertEquals(1, queue.pendingCount())
                dispatcher.runAll()
                assertEquals(TaskState.Completed(true), first.currentState)
                assertTrue(owner.isActive)
            } finally {
                queue.shutdown()
                owner.cancel()
                dispatcher.runAll()
            }
        }
    }

    @Test fun synchronousStateCollectorRejectsSaturatedBlockReentry() {
        for (concurrent in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.block(1))
            queue.pause()
            val attempted = AtomicBoolean(false)
            val nestedResult = AtomicReference<EnqueueResult>()
            val collector = owner.launch(Dispatchers.Unconfined) {
                queue.state.collect { state ->
                    val full = state is ConcurrentTaskQueue.ConcurrentState.Full ||
                        state is SerialTaskQueue.QueueState.Paused && state.pendingCount == 1
                    if (full && attempted.compareAndSet(false, true)) {
                        nestedResult.set(queue.enqueueWithResult(Task(block = { fail("Rejected task must not run") })))
                    }
                }
            }
            val first = Task(block = { Unit })
            try {
                onProducerThread(queue) { queue.enqueue(first) }
                assertTrue(attempted.get())
                assertCapacityRejection(nestedResult.get())
                assertEquals("concurrent=$concurrent state=${queue.state.value}", 1, queue.pendingCount())
                queue.resume()
                dispatcher.runAll()
                assertEquals(TaskState.Completed(true), first.currentState)
                assertTrue(owner.isActive)
            } finally {
                collector.cancel()
                queue.shutdown()
                owner.cancel()
                dispatcher.runAll()
            }
        }
    }

    @Test fun unrelatedPendingCallbackCancellationDoesNotCancelThePolledTask() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.unbounded())
            var failDuringDispatch = false
            val completed = mutableListOf<String>()
            val first = Task(block = { completed += "first" })
            val second = Task(block = { completed += "second" }, onStateChange = { state ->
                if (failDuringDispatch && state is TaskState.Pending && state.queueSize == 1) {
                    throw CancellationException("unrelated pending task callback")
                }
            })
            try {
                queue.enqueue(first)
                queue.enqueue(second)
                failDuringDispatch = true
                runCurrent()
                assertEquals(listOf("first", "second"), completed)
                assertEquals(TaskState.Completed(true), first.currentState)
                assertEquals(TaskState.Completed(true), second.currentState)
                assertTrue(owner.isActive)
                assertTrue(queue.isIdle())
            } finally {
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    @Test fun suspendedTaskRetainsBlockReentryProtectionAfterChangingDispatcher() {
        for (concurrent in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val switchedDispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.block(1))
            val resumeTask = CompletableDeferred<Unit>()
            val nestedResult = AtomicReference<EnqueueResult>()
            val convenienceFailure = AtomicReference<Throwable>()
            val completed = mutableListOf<String>()
            var firstStarted = false
            val first = Task(block = {
                firstStarted = true
                resumeTask.await()
                withContext(switchedDispatcher) {
                    nestedResult.set(queue.enqueueWithResult(Task(block = { fail("Rejected task must not run") })))
                    convenienceFailure.set(runCatching {
                        queue.enqueue(Task(block = { fail("Rejected task must not run") }))
                    }.exceptionOrNull())
                }
                completed += "first"
            })
            val second = Task(block = { completed += "second" })
            try {
                queue.enqueue(first)
                dispatcher.runAll()
                assertTrue(firstStarted)
                assertTrue(first.currentState is TaskState.Running)
                queue.enqueue(second)
                assertEquals(1, queue.pendingCount())

                resumeTask.complete(Unit)
                dispatcher.runAll()
                assertNull(nestedResult.get())
                assertEquals(1, queue.pendingCount())
                onProducerThread(queue) {
                    switchedDispatcher.runAll()
                    dispatcher.runAll()
                }

                assertCapacityRejection(nestedResult.get())
                assertTrue(convenienceFailure.get() is TaskQueueFullException)
                assertEquals(listOf("first", "second"), completed)
                assertEquals(TaskState.Completed(true), first.currentState)
                assertEquals(TaskState.Completed(true), second.currentState)
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                resumeTask.complete(Unit)
                queue.shutdown()
                owner.cancel()
                switchedDispatcher.runAll()
                dispatcher.runAll()
            }
        }
    }

    @Test fun awaitingCompletionFromInitialPendingCallbackWaitsForAcceptedWork() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val queue = ConcurrentTaskQueue(backgroundScope, 1, dispatcher)
        var taskRan = false
        var callbackObserved = false
        lateinit var waiter: Deferred<Unit>
        val task = Task(block = { taskRan = true }, onStateChange = { state ->
            if (state is TaskState.Pending && !callbackObserved) {
                callbackObserved = true
                waiter = async(start = CoroutineStart.UNDISPATCHED) { queue.awaitCompletion() }
            }
        })
        try {
            queue.enqueue(task)
            assertTrue(callbackObserved)
            assertEquals(1, queue.pendingTaskCount)
            assertFalse(taskRan)
            assertFalse(waiter.isCompleted)
            runCurrent()
            assertTrue(taskRan)
            assertEquals(TaskState.Completed(true), task.currentState)
            assertTrue(waiter.isCompleted)
            waiter.await()
            assertTrue(queue.isIdle)
        } finally {
            queue.shutdown()
            runCurrent()
        }
    }

    private fun assertCapacityRejection(result: EnqueueResult?) {
        assertTrue("Saturated reentry must return a capacity rejection: $result", result is EnqueueResult.Rejected)
        assertEquals(EnqueueResult.RejectionCause.CAPACITY, (result as EnqueueResult.Rejected).cause)
    }

    private fun onProducerThread(queue: QueueHarness, action: () -> Unit) {
        val outcome = FutureTask(action)
        val producer = Thread(outcome, "queue-notification-reentry").apply { isDaemon = true }
        var completed = false
        try {
            producer.start()
            outcome.get(5, TimeUnit.SECONDS)
            completed = true
        } finally {
            if (!completed && producer.isAlive) {
                queue.shutdown()
                producer.interrupt()
            }
            producer.join(5_000)
        }
    }

    private fun queue(
        owner: CoroutineScope,
        dispatcher: CoroutineDispatcher,
        concurrent: Boolean,
        config: BackpressureConfig,
    ): QueueHarness = if (concurrent) {
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher, config)
        QueueHarness(
            { queue.enqueue(it); Unit }, queue::enqueueWithResult,
            { queue.pendingTaskCount }, { queue.isIdle }, queue::shutdown, queue.state,
        )
    } else {
        val queue = SerialTaskQueue(owner, dispatcher, config)
        QueueHarness(
            { queue.enqueue(it); Unit }, queue::enqueueWithResult,
            { queue.pendingCount }, { queue.isIdle }, queue::shutdown, queue.state,
            queue::pause, queue::resume,
        )
    }

    private class QueueHarness(
        val enqueue: (Task<Unit>) -> Unit,
        val enqueueWithResult: (Task<Unit>) -> EnqueueResult,
        val pendingCount: () -> Int,
        val isIdle: () -> Boolean,
        val shutdown: () -> Unit,
        val state: StateFlow<*>,
        val pause: () -> Unit = {},
        val resume: () -> Unit = {},
    )

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
