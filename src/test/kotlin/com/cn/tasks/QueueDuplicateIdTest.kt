package com.cn.tasks

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.coroutines.CoroutineContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueDuplicateIdTest {
    @Test fun duplicatePendingIdsAreRejectedBeforeDiscardingTheOriginal() = runTest {
        for (concurrent in listOf(false, true)) {
            val queue = queue(this, StandardTestDispatcher(testScheduler), concurrent, BackpressureConfig.discardOldest(1))
            val original = Task(id = "shared", block = { Unit })
            try {
                queue.enqueue(original)
                assertTrue(runCatching { queue.enqueue(original) }.exceptionOrNull() is IllegalArgumentException)
                assertTrue(runCatching {
                    queue.enqueue(Task(id = original.id, block = { Unit }))
                }.exceptionOrNull() is IllegalArgumentException)
                assertFalse(original.isCancelled)
                assertTrue(original.currentState is TaskState.Pending)
                assertEquals(1, queue.pending())
            } finally {
                queue.shutdown()
                runCurrent()
            }
        }
    }

    @Test fun duplicateRunningIdsDoNotReplaceTheJobOwnedByShutdown() = runTest {
        for (concurrent in listOf(false, true)) {
            val queue = queue(this, StandardTestDispatcher(testScheduler), concurrent, BackpressureConfig.unbounded())
            var exits = 0
            val original = Task<Unit>(id = "running", block = {
                try { awaitCancellation() } finally { exits++ }
            })
            try {
                queue.enqueue(original)
                runCurrent()
                assertTrue(runCatching {
                    queue.enqueue(Task(id = original.id, block = { Unit }))
                }.exceptionOrNull() is IllegalArgumentException)
                assertFalse(original.isCancelled)

                queue.shutdown()
                runCurrent()

                assertEquals(1, exits)
                assertEquals(TaskState.Cancelled, original.currentState)
                assertTrue(isActive)
            } finally {
                queue.shutdown()
                runCurrent()
            }
        }
    }

    @Test fun duplicateIdsAreRejectedDuringThePollToJobHandoff() = runTest {
        for (concurrent in listOf(false, true)) {
            val queue = queue(this, StandardTestDispatcher(testScheduler), concurrent, BackpressureConfig.unbounded())
            var attempted = false
            var failure: Throwable? = null
            val original = Task<Unit>(id = "handoff", block = { awaitCancellation() })
            val next = Task(block = { Unit }, onStateChange = {
                if (it is TaskState.Pending && queue.pending() == 1 && !attempted) {
                    attempted = true
                    failure = runCatching {
                        queue.enqueue(Task(id = original.id, block = { Unit }))
                    }.exceptionOrNull()
                }
            })
            try {
                queue.enqueue(original)
                queue.enqueue(next)
                runCurrent()

                assertTrue(attempted)
                assertTrue(failure is IllegalArgumentException)
                assertFalse(original.isCancelled)
            } finally {
                queue.shutdown()
                runCurrent()
            }
        }
    }

    @Test fun callerRunsIdsRemainOwnedUntilTheCallerExecutionExits() = runTest {
        for (concurrent in listOf(false, true)) {
            val queue = queue(this, StandardTestDispatcher(testScheduler), concurrent, BackpressureConfig.callerRuns(1))
            lateinit var caller: Task<Unit>
            var duplicateFailure: Throwable? = null
            caller = Task(id = "caller", block = {
                duplicateFailure = runCatching { queue.enqueue(caller) }.exceptionOrNull()
            })
            try {
                queue.enqueue(Task(block = { Unit }))
                val result = queue.enqueue(caller)

                assertTrue(duplicateFailure is IllegalArgumentException)
                assertTrue((result as EnqueueResult.CallerExecuted).success)
                assertFalse(caller.isCancelled)
                assertEquals(TaskState.Completed(true), caller.currentState)
            } finally {
                queue.shutdown()
                runCurrent()
            }
        }
    }

    @Test fun blockedProducersRecheckIdsAfterCapacityBecomesAvailable() {
        for (concurrent in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.block(1))
            val filler = Task(block = { Unit })
            queue.enqueue(filler)
            val results = List(2) {
                FutureTask { runCatching { queue.enqueue(Task(id = "blocked", block = { Unit })) } }
            }
            val producers = results.mapIndexed { index, result -> Thread(result, "duplicate-block-$index") }
            try {
                producers.forEach { it.start() }
                producers.forEach(::awaitWaiting)
                assertTrue(queue.cancel(filler.id))

                val outcomes = results.map { it.get(5, TimeUnit.SECONDS) }

                assertEquals(1, outcomes.count { it.isSuccess })
                assertEquals(1, outcomes.count { it.exceptionOrNull() is IllegalArgumentException })
                assertEquals(1, queue.pending())
                assertTrue(owner.isActive)
            } finally {
                queue.shutdown()
                owner.cancel()
                producers.forEach { it.interrupt() }
                producers.forEach { it.join(5_000) }
                dispatcher.runAll()
            }
        }
    }

    private fun awaitWaiting(thread: Thread) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        // Condition.await 允许伪唤醒，生产者可能在两次读取之间离开 WAITING；断言必须落在循环观察到的那一次读取上。
        var state = thread.state
        while (state != Thread.State.WAITING && System.nanoTime() < deadline) {
            Thread.yield()
            state = thread.state
        }
        assertEquals(Thread.State.WAITING, state)
    }

    private fun queue(
        owner: CoroutineScope,
        dispatcher: CoroutineDispatcher,
        concurrent: Boolean,
        config: BackpressureConfig,
    ): QueueAccess = if (concurrent) {
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher, config)
        QueueAccess(queue::enqueueWithResult, queue::cancel, queue::shutdown, { queue.pendingTaskCount })
    } else {
        val queue = SerialTaskQueue(owner, dispatcher, config)
        QueueAccess(queue::enqueueWithResult, queue::cancel, queue::shutdown, { queue.pendingCount })
    }

    private class QueueAccess(
        val enqueue: (Task<Unit>) -> EnqueueResult,
        val cancel: (String) -> Boolean,
        val shutdown: () -> Unit,
        val pending: () -> Int,
    )

    private class PausedDispatcher : CoroutineDispatcher() {
        private val work = ConcurrentLinkedQueue<Runnable>()
        override fun dispatch(context: CoroutineContext, block: Runnable) { work.add(block) }
        fun runAll() { while (true) (work.poll() ?: return).run() }
    }
}
