package com.cn.tasks

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import org.junit.Assert.*
import org.junit.Test

class ConcurrentDispatchFailureTest {
    @Test fun fatalPendingNotificationDoesNotStartReservedWorkEvenWithAnImmediateDispatcher() {
        for (immediate in listOf(false, true)) verifyFatalHandoff(immediate, cleanupFailure = null)
    }

    @Test fun reservationCancellationFailuresRemainSuppressedBehindTheOriginalFatalNotification() {
        for (immediate in listOf(false, true)) {
            for (cleanupFailure in listOf(
                CancellationException("cancelled observer cancellation"),
                LinkageError("cancelled observer fatal failure"),
                object : Throwable("cancelled observer non-exception failure") {},
            )) {
                verifyFatalHandoff(immediate, cleanupFailure)
            }
        }
    }

    @Test fun shutdownFromTheCancelledReservationCallbackTerminatesWithoutHidingTheFatalFailure() {
        for (immediate in listOf(false, true)) {
            val ownerDispatcher = PausedDispatcher()
            val failures = ConcurrentLinkedQueue<Throwable>()
            val owner = CoroutineScope(SupervisorJob() + ownerDispatcher + CoroutineExceptionHandler { _, error ->
                failures.add(error)
            })
            val taskDispatcher = if (immediate) Dispatchers.Unconfined else ownerDispatcher
            val queue = ConcurrentTaskQueue(owner, 1, taskDispatcher)
            val fatal = AssertionError("fatal pending notification")
            val cancellation = CancellationException("cancelled observer after shutdown")
            var armed = false
            var cancellations = 0
            var completions = 0
            val reserved = Task<Unit>(
                block = { fail("A failed reservation must never execute") },
                onStateChange = { state ->
                    if (state is TaskState.Cancelled) {
                        cancellations++
                        queue.shutdown()
                        throw cancellation
                    }
                },
                onComplete = { completions++ },
            )
            val remaining = Task<Unit>(
                block = { fail("Shutdown must cancel pending work") },
                onStateChange = { state ->
                    if (armed && state is TaskState.Pending && state.queueSize == 1) throw fatal
                },
            )
            val outcome = FutureTask { ownerDispatcher.runAll() }
            val worker = Thread(outcome, "concurrent-fatal-shutdown").apply { isDaemon = true }
            var completed = false
            try {
                queue.enqueue(reserved)
                queue.enqueue(remaining)
                armed = true
                worker.start()
                outcome.get(5, TimeUnit.SECONDS)
                completed = true

                assertEquals(listOf(fatal), failures.toList())
                assertEquals(listOf(cancellation), fatal.suppressed.toList())
                assertEquals(TaskState.Cancelled, reserved.currentState)
                assertEquals(TaskState.Cancelled, remaining.currentState)
                assertEquals(1, cancellations)
                assertEquals(0, completions)
                assertEquals(0, queue.runningTaskCount)
                assertEquals(0, queue.pendingTaskCount)
                assertEquals(ConcurrentTaskQueue.ConcurrentState.Shutdown, queue.state.value)
                assertTrue(queue.isTerminated)
                assertTrue(owner.isActive)
            } finally {
                if (!completed && worker.isAlive) {
                    queue.shutdown()
                    worker.interrupt()
                }
                worker.join(5_000)
                queue.shutdown()
                owner.cancel()
                ownerDispatcher.runAll()
            }
        }
    }

    private fun verifyFatalHandoff(immediate: Boolean, cleanupFailure: Throwable?) {
        val ownerDispatcher = PausedDispatcher()
        val failures = ConcurrentLinkedQueue<Throwable>()
        val owner = CoroutineScope(SupervisorJob() + ownerDispatcher + CoroutineExceptionHandler { _, error ->
            failures.add(error)
        })
        val taskDispatcher = if (immediate) Dispatchers.Unconfined else ownerDispatcher
        val queue = ConcurrentTaskQueue(owner, 1, taskDispatcher)
        val releaseBusiness = CountDownLatch(1)
        val releaseReplacement = CompletableDeferred<Unit>()
        val fatal = AssertionError("fatal pending notification")
        var armed = false
        var notifiedFatally = false
        var businessStarted = false
        var cancellations = 0
        var completions = 0
        val reserved = Task<Unit>(
            block = {
                businessStarted = true
                check(releaseBusiness.await(10, TimeUnit.SECONDS))
            },
            onStateChange = { state ->
                if (state is TaskState.Cancelled) {
                    cancellations++
                    cleanupFailure?.let { throw it }
                }
            },
            onComplete = { completions++ },
        )
        val remaining = Task(block = { Unit }, onStateChange = { state ->
            if (armed && !notifiedFatally && state is TaskState.Pending && state.queueSize == 1) {
                notifiedFatally = true
                throw fatal
            }
        })
        val outcome = FutureTask { ownerDispatcher.runAll() }
        val worker = Thread(outcome, "concurrent-fatal-notification").apply { isDaemon = true }
        var completed = false
        try {
            queue.enqueue(reserved)
            queue.enqueue(remaining)
            armed = true
            worker.start()
            outcome.get(5, TimeUnit.SECONDS)
            completed = true

            assertTrue(notifiedFatally)
            assertEquals("The fatal notification must not wait for another business body", 1L, releaseBusiness.count)
            assertEquals(listOf(fatal), failures.toList())
            assertEquals(if (cleanupFailure == null) emptyList<Throwable>() else listOf(cleanupFailure),
                fatal.suppressed.toList())
            assertFalse("A failed reservation must never start its business body", businessStarted)
            assertTrue(reserved.isCancelled)
            assertEquals(TaskState.Cancelled, reserved.currentState)
            assertEquals(1, cancellations)
            assertEquals(0, completions)
            assertEquals(TaskState.Completed(true), remaining.currentState)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, queue.pendingTaskCount)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)

            var replacementStarted = false
            var followingStarted = false
            val replacement = queue.enqueue(Task(id = reserved.id, block = {
                replacementStarted = true
                releaseReplacement.await()
            }))
            val following = queue.enqueue(Task(block = { followingStarted = true }))
            ownerDispatcher.runAll()

            assertTrue("The recovered permit must let replacement work start", replacementStarted)
            assertFalse("A permit must not be returned twice and bypass maxConcurrency", followingStarted)
            assertEquals(1, queue.runningTaskCount)
            assertEquals(1, queue.pendingTaskCount)
            releaseReplacement.complete(Unit)
            ownerDispatcher.runAll()

            assertEquals(TaskState.Completed(true), replacement.currentState)
            assertEquals(TaskState.Completed(true), following.currentState)
            assertTrue(followingStarted)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, queue.pendingTaskCount)
            assertEquals(1, failures.size)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)
        } finally {
            releaseBusiness.countDown()
            releaseReplacement.complete(Unit)
            if (!completed && worker.isAlive) {
                queue.shutdown()
                worker.interrupt()
            }
            worker.join(5_000)
            queue.shutdown()
            owner.cancel()
            ownerDispatcher.runAll()
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
