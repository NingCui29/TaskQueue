package com.cn.tasks

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueCompletionCallbackTest {
    @Test fun completionCallbackRejectsSaturatedBlockReentryWithoutBlockingItsDispatcher() {
        val dispatcher = PausedDispatcher()
        val owner = CoroutineScope(SupervisorJob() + dispatcher)
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher, BackpressureConfig.block(1))
        val accepted = AtomicReference<EnqueueResult>()
        val rejected = AtomicReference<EnqueueResult>()
        val convenienceFailure = AtomicReference<Throwable>()
        val first = Task(block = { Unit })
        queue.awaitCompletion {
            accepted.set(queue.enqueueWithResult(first))
            rejected.set(queue.enqueueWithResult(Task(block = { fail("Rejected work must not execute") })))
            convenienceFailure.set(runCatching {
                queue.enqueue(Task(block = { fail("Rejected work must not execute") }))
            }.exceptionOrNull())
        }
        val outcome = FutureTask { dispatcher.runAll() }
        val runner = Thread(outcome, "completion-callback-reentry").apply { isDaemon = true }
        var completed = false
        try {
            runner.start()
            outcome.get(5, TimeUnit.SECONDS)
            completed = true

            assertTrue(accepted.get() is EnqueueResult.Success)
            assertTrue(rejected.get() is EnqueueResult.Rejected)
            assertEquals(EnqueueResult.RejectionCause.CAPACITY,
                (rejected.get() as EnqueueResult.Rejected).cause)
            assertTrue(convenienceFailure.get() is TaskQueueFullException)
            assertEquals(TaskState.Completed(true), first.currentState)
            assertEquals(0, queue.pendingTaskCount)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)
        } finally {
            if (!completed && runner.isAlive) {
                queue.shutdown()
                runner.interrupt()
            }
            runner.join(5_000)
            queue.shutdown()
            owner.cancel()
            dispatcher.runAll()
        }
    }

    @Test fun completionCallbackWaitsForCancelledWorkToFinishCleanup() = runTest {
        val queue = ConcurrentTaskQueue(this, 1, StandardTestDispatcher(testScheduler))
        val releaseCleanup = CompletableDeferred<Unit>()
        var cleaning = false
        var completions = 0
        val task = Task<Unit>(block = {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    cleaning = true
                    releaseCleanup.await()
                }
            }
        })
        try {
            queue.enqueue(task)
            runCurrent()
            queue.awaitCompletion { completions++ }
            queue.cancelAll()
            runCurrent()

            assertTrue(cleaning)
            assertEquals(0, completions)
            assertEquals(1, queue.runningTaskCount)
            assertFalse(queue.isIdle)

            releaseCleanup.complete(Unit)
            runCurrent()

            assertEquals(1, completions)
            assertEquals(TaskState.Cancelled, task.currentState)
            assertTrue(queue.isIdle)
            assertTrue(isActive)
        } finally {
            releaseCleanup.complete(Unit)
            queue.shutdown()
            runCurrent()
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
