package com.cn.tasks.validation

import android.os.Looper
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.cn.tasks.BackpressureConfig
import com.cn.tasks.ConcurrentTaskQueue
import com.cn.tasks.EnqueueResult
import com.cn.tasks.SerialTaskQueue
import com.cn.tasks.Task
import com.cn.tasks.TaskState
import com.cn.tasks.log.TaskLog
import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

/** Device acceptance against the built library JAR and real Android/coroutine dispatchers. */
@RunWith(AndroidJUnit4::class)
class RealDispatcherDeviceTest {
    @Test(timeout = 15_000L)
    fun serialMainOwnerDispatchesBusinessProgressAndSuccessToDefault() = runBlocking {
        withTimeout(10_000L) { verifyMainOwner(concurrent = false) }
    }

    @Test(timeout = 15_000L)
    fun concurrentMainOwnerDispatchesBusinessProgressAndSuccessToDefault() = runBlocking {
        withTimeout(10_000L) { verifyMainOwner(concurrent = true) }
    }

    @Test(timeout = 15_000L)
    fun serialDefaultPendingCancellationAndDiscardMatrix() = runBlocking {
        withTimeout(10_000L) { verifyPendingCancellation(concurrent = false) }
    }

    @Test(timeout = 15_000L)
    fun concurrentDefaultPendingCancellationAndDiscardMatrix() = runBlocking {
        withTimeout(10_000L) { verifyPendingCancellation(concurrent = true) }
    }

    @Test(timeout = 15_000L)
    fun serialIoCompletionWaitsForNonCancellableCleanup() = runBlocking {
        withTimeout(10_000L) { verifyCleanup(concurrent = false) }
    }

    @Test(timeout = 15_000L)
    fun concurrentIoCompletionWaitsForNonCancellableCleanup() = runBlocking {
        withTimeout(10_000L) { verifyCleanup(concurrent = true) }
    }

    @Test(timeout = 15_000L)
    fun serialDefaultCancellationCallbacksCanReenterWithoutDuplicateCompletion() = runBlocking {
        withTimeout(10_000L) { verifyCancellationReentry(concurrent = false) }
    }

    @Test(timeout = 15_000L)
    fun concurrentDefaultCancellationCallbacksCanReenterWithoutDuplicateCompletion() = runBlocking {
        withTimeout(10_000L) { verifyCancellationReentry(concurrent = true) }
    }

    @Test(timeout = 15_000L)
    fun serialDefaultSuccessSurvivesOrdinaryObserverAndLoggerFailures() = runBlocking {
        withTimeout(10_000L) { verifyLoggerIsolation(concurrent = false) }
    }

    @Test(timeout = 15_000L)
    fun concurrentDefaultSuccessSurvivesOrdinaryObserverAndLoggerFailures() = runBlocking {
        withTimeout(10_000L) { verifyLoggerIsolation(concurrent = true) }
    }

    private suspend fun verifyMainOwner(concurrent: Boolean) {
        val failures = ConcurrentLinkedQueue<Throwable>()
        val owner = owner(Dispatchers.Main.immediate, failures)
        val queue = DeviceQueue(owner, Dispatchers.Default, concurrent)
        val completed = CompletableDeferred<Unit>()
        val received = CompletableDeferred<Int>()
        val initialPendingOnMain = AtomicReference<Boolean>()
        val bodyThread = AtomicReference<Thread>()
        val progressThread = AtomicReference<Thread>()
        val successThread = AtomicReference<Thread>()
        val bodyOnMain = AtomicBoolean(true)
        val progressOnMain = AtomicBoolean(true)
        val successOnMain = AtomicBoolean(true)
        val completions = AtomicInteger()
        val task = Task<Int>(
            name = "main-owner-default-business",
            block = { progress ->
                bodyThread.set(Thread.currentThread())
                bodyOnMain.set(onMainThread())
                progress.report(0.5f, "halfway")
                42
            },
            onStateChange = { state ->
                if (state is TaskState.Pending) initialPendingOnMain.compareAndSet(null, onMainThread())
                if (state is TaskState.Running && state.message == "halfway") {
                    progressThread.set(Thread.currentThread())
                    progressOnMain.set(onMainThread())
                }
            },
            onSuccess = { value ->
                successThread.set(Thread.currentThread())
                successOnMain.set(onMainThread())
                received.complete(value)
            },
            onComplete = { completions.incrementAndGet(); completed.complete(Unit) },
        )
        try {
            withContext(Dispatchers.Main.immediate) {
                assertSame(Looper.getMainLooper(), Looper.myLooper())
                assertTrue(queue.enqueue(task) is EnqueueResult.Success)
            }
            completed.await()
            queue.awaitIdle()

            assertEquals(42, received.await())
            assertEquals(TaskState.Completed(true), task.currentState)
            assertEquals(true, initialPendingOnMain.get())
            assertFalse(bodyOnMain.get())
            assertFalse(progressOnMain.get())
            assertFalse(successOnMain.get())
            assertSame(bodyThread.get(), progressThread.get())
            assertSame(bodyThread.get(), successThread.get())
            assertEquals(1, completions.get())
            assertTrue(owner.isActive)
            assertTrue(failures.toString(), failures.isEmpty())
        } finally {
            close(queue, owner)
        }
    }

    private suspend fun verifyPendingCancellation(concurrent: Boolean) {
        for (action in PendingAction.entries) {
            val failures = ConcurrentLinkedQueue<Throwable>()
            val owner = owner(Dispatchers.Default, failures)
            val config = if (action == PendingAction.DISCARD_OLDEST) {
                BackpressureConfig.discardOldest(1)
            } else BackpressureConfig.unbounded()
            val queue = DeviceQueue(owner, Dispatchers.Default, concurrent, config)
            val blockerStarted = CompletableDeferred<Unit>()
            val releaseBlocker = CompletableDeferred<Unit>()
            val blockerCompleted = CompletableDeferred<Unit>()
            val victimCompleted = CompletableDeferred<Unit>()
            val victimRuns = AtomicInteger()
            val victimCompletions = AtomicInteger()
            val victimCancellations = AtomicInteger()
            val replacementRuns = AtomicInteger()
            val blocker = Task<Unit>(
                name = "blocker-$action",
                block = { blockerStarted.complete(Unit); releaseBlocker.await() },
                onComplete = { blockerCompleted.complete(Unit) },
            )
            val victim = Task<Unit>(
                name = "pending-$action",
                block = { victimRuns.incrementAndGet() },
                onStateChange = { if (it is TaskState.Cancelled) victimCancellations.incrementAndGet() },
                onComplete = { victimCompletions.incrementAndGet(); victimCompleted.complete(Unit) },
            )
            val replacement = Task<Unit>(block = { replacementRuns.incrementAndGet() })
            try {
                queue.enqueue(blocker)
                blockerStarted.await()
                queue.enqueue(victim)
                assertEquals(action.toString(), 1, queue.pendingCount)
                assertTrue(victim.currentState is TaskState.Pending)
                assertEquals(0, victimCompletions.get())

                withContext(Dispatchers.Default) {
                    when (action) {
                        PendingAction.CANCEL -> assertTrue(queue.cancel(victim.id))
                        PendingAction.CANCEL_ALL -> queue.cancelAll()
                        PendingAction.SHUTDOWN -> queue.shutdown()
                        PendingAction.DISCARD_OLDEST -> {
                            val result = queue.enqueue(replacement)
                            assertTrue(result is EnqueueResult.Discarded)
                            assertSame(victim, (result as EnqueueResult.Discarded).discardedTask)
                        }
                    }
                }
                victimCompleted.await()
                releaseBlocker.complete(Unit)
                blockerCompleted.await()
                queue.awaitIdle()

                assertEquals(action.toString(), TaskState.Cancelled, victim.currentState)
                assertEquals(0, victimRuns.get())
                assertEquals(1, victimCancellations.get())
                assertEquals(1, victimCompletions.get())
                assertFalse(queue.cancel(victim.id))
                assertEquals(if (action == PendingAction.DISCARD_OLDEST) 1 else 0, replacementRuns.get())
                queue.cancelAll()
                queue.shutdown()
                queue.awaitIdle()
                assertEquals(1, victimCompletions.get())
                assertTrue(owner.isActive)
                assertTrue(failures.toString(), failures.isEmpty())
            } finally {
                releaseBlocker.complete(Unit)
                close(queue, owner)
            }
        }
    }

    private suspend fun verifyCleanup(concurrent: Boolean) {
        for (shutdown in listOf(false, true)) {
            val failures = ConcurrentLinkedQueue<Throwable>()
            val owner = owner(Dispatchers.Default, failures)
            val queue = DeviceQueue(owner, Dispatchers.IO, concurrent)
            val started = CompletableDeferred<Unit>()
            val holdBody = CompletableDeferred<Unit>()
            val cleanupStarted = CompletableDeferred<Unit>()
            val releaseCleanup = CompletableDeferred<Unit>()
            val completed = CompletableDeferred<Unit>()
            val cleaned = AtomicBoolean(false)
            val completedAfterCleanup = AtomicBoolean(false)
            val bodyOnMain = AtomicBoolean(true)
            val completions = AtomicInteger()
            val task = Task<Unit>(
                block = {
                    bodyOnMain.set(onMainThread())
                    started.complete(Unit)
                    try {
                        holdBody.await()
                    } finally {
                        withContext(NonCancellable) {
                            cleanupStarted.complete(Unit)
                            releaseCleanup.await()
                            cleaned.set(true)
                        }
                    }
                },
                onComplete = {
                    completedAfterCleanup.set(cleaned.get())
                    completions.incrementAndGet()
                    completed.complete(Unit)
                },
            )
            try {
                queue.enqueue(task)
                started.await()
                if (shutdown) queue.shutdown() else queue.cancelAll()
                cleanupStarted.await()
                queue.cancelAll()

                assertFalse(bodyOnMain.get())
                assertFalse(completed.isCompleted)
                assertEquals(0, completions.get())
                assertEquals(1, queue.runningCount)
                assertFalse(queue.isIdle)
                assertTrue(owner.isActive)

                releaseCleanup.complete(Unit)
                completed.await()
                queue.awaitIdle()

                assertTrue(completedAfterCleanup.get())
                assertEquals(1, completions.get())
                assertEquals(TaskState.Cancelled, task.currentState)
                assertEquals(0, queue.runningCount)
                assertTrue(failures.toString(), failures.isEmpty())
            } finally {
                holdBody.complete(Unit)
                releaseCleanup.complete(Unit)
                close(queue, owner)
            }
        }
    }

    private suspend fun verifyCancellationReentry(concurrent: Boolean) {
        val failures = ConcurrentLinkedQueue<Throwable>()
        val owner = owner(Dispatchers.Default, failures)
        val queue = DeviceQueue(owner, Dispatchers.Default, concurrent)
        val blockerStarted = CompletableDeferred<Unit>()
        val releaseBlocker = CompletableDeferred<Unit>()
        val victimCompleted = CompletableDeferred<Unit>()
        val followingCompleted = CompletableDeferred<Unit>()
        val victimRuns = AtomicInteger()
        val followingRuns = AtomicInteger()
        val cancellations = AtomicInteger()
        val completions = AtomicInteger()
        val followingCompletions = AtomicInteger()
        val victim = Task<Unit>(
            block = { victimRuns.incrementAndGet() },
            onStateChange = {
                if (it is TaskState.Cancelled) {
                    cancellations.incrementAndGet()
                    queue.cancelAll()
                }
            },
            onComplete = {
                completions.incrementAndGet()
                queue.cancelAll()
                victimCompleted.complete(Unit)
            },
        )
        val following = Task<Unit>(
            block = { followingRuns.incrementAndGet() },
            onComplete = { followingCompletions.incrementAndGet(); followingCompleted.complete(Unit) },
        )
        try {
            queue.enqueue(Task<Unit>(block = { blockerStarted.complete(Unit); releaseBlocker.await() }))
            blockerStarted.await()
            queue.enqueue(victim)
            queue.enqueue(following)
            assertEquals(2, queue.pendingCount)

            withContext(Dispatchers.Default) { assertTrue(queue.cancel(victim.id)) }
            victimCompleted.await()
            followingCompleted.await()
            releaseBlocker.complete(Unit)
            queue.awaitIdle()

            assertEquals(0, victimRuns.get())
            assertEquals(0, followingRuns.get())
            assertEquals(1, cancellations.get())
            assertEquals(1, completions.get())
            assertEquals(1, followingCompletions.get())
            assertEquals(TaskState.Cancelled, victim.currentState)
            assertEquals(TaskState.Cancelled, following.currentState)
            assertEquals(0, queue.runningCount)
            assertTrue(owner.isActive)
            assertTrue(failures.toString(), failures.isEmpty())
        } finally {
            releaseBlocker.complete(Unit)
            close(queue, owner)
        }
    }

    private suspend fun verifyLoggerIsolation(concurrent: Boolean) {
        val previousEnabled = TaskLog.enabled
        val previousPriority = TaskLog.minPriority
        val previousPrinter = TaskLog.printer
        val failures = ConcurrentLinkedQueue<Throwable>()
        val owner = owner(Dispatchers.Default, failures)
        val queue = DeviceQueue(owner, Dispatchers.Default, concurrent)
        val completed = CompletableDeferred<Unit>()
        val received = AtomicInteger(-1)
        val errors = AtomicInteger()
        val completions = AtomicInteger()
        val logAttempts = AtomicInteger()
        val successOnMain = AtomicBoolean(true)
        val task = Task<Int>(
            block = { 42 },
            onSuccess = {
                received.set(it)
                successOnMain.set(onMainThread())
                throw IOException("ordinary success observer failure")
            },
            onError = { errors.incrementAndGet() },
            onComplete = {
                completions.incrementAndGet()
                completed.complete(Unit)
                throw IOException("ordinary completion observer failure")
            },
        )
        try {
            TaskLog.minPriority = TaskLog.PRIORITY_ERROR
            TaskLog.printer = TaskLog.Printer { _, _, _, _ ->
                logAttempts.incrementAndGet()
                throw IOException("ordinary logger failure")
            }
            TaskLog.enabled = true
            assertTrue(queue.enqueue(task) is EnqueueResult.Success)
            completed.await()
            queue.awaitIdle()

            assertEquals(42, received.get())
            assertFalse(successOnMain.get())
            assertEquals(TaskState.Completed(true), task.currentState)
            assertEquals(0, errors.get())
            assertEquals(1, completions.get())
            assertEquals(2, logAttempts.get())
            assertTrue(owner.isActive)
            assertTrue(failures.toString(), failures.isEmpty())
        } finally {
            try {
                close(queue, owner)
            } finally {
                TaskLog.enabled = false
                TaskLog.printer = previousPrinter
                TaskLog.minPriority = previousPriority
                TaskLog.enabled = previousEnabled
            }
        }
    }

    private fun owner(
        dispatcher: CoroutineDispatcher,
        failures: ConcurrentLinkedQueue<Throwable>,
    ): CoroutineScope = CoroutineScope(
        SupervisorJob() + dispatcher + CoroutineExceptionHandler { _, failure -> failures.add(failure) },
    )

    private suspend fun close(queue: DeviceQueue, owner: CoroutineScope) {
        try {
            queue.shutdown()
        } finally {
            owner.cancel()
            withContext(NonCancellable) {
                withTimeoutOrNull(1_000L) { owner.coroutineContext[Job]?.join() }
            }
        }
    }

    private fun onMainThread(): Boolean = Looper.myLooper() === Looper.getMainLooper()

    private enum class PendingAction { CANCEL, CANCEL_ALL, SHUTDOWN, DISCARD_OLDEST }

    private class DeviceQueue(
        owner: CoroutineScope,
        dispatcher: CoroutineDispatcher,
        concurrent: Boolean,
        config: BackpressureConfig = BackpressureConfig.unbounded(),
    ) {
        private val serial = if (concurrent) null else SerialTaskQueue(owner, dispatcher, config)
        private val parallel = if (concurrent) ConcurrentTaskQueue(owner, 1, dispatcher, config) else null

        val pendingCount: Int get() = serial?.pendingCount ?: parallel!!.pendingTaskCount
        val runningCount: Int get() = serial?.runningTaskCount ?: parallel!!.runningTaskCount
        val isIdle: Boolean get() = serial?.isIdle ?: parallel!!.isIdle

        fun <T> enqueue(task: Task<T>): EnqueueResult =
            serial?.enqueueWithResult(task) ?: parallel!!.enqueueWithResult(task)

        fun cancel(taskId: String): Boolean = serial?.cancel(taskId) ?: parallel!!.cancel(taskId)

        fun cancelAll() {
            if (serial != null) serial.cancelAll() else parallel!!.cancelAll()
        }

        fun shutdown() {
            if (serial != null) serial.shutdown() else parallel!!.shutdown()
        }

        suspend fun awaitIdle() {
            if (parallel != null) {
                parallel.awaitCompletion()
            } else {
                // The serial public API has no awaitCompletion; bounded by the enclosing test timeout.
                while (!isIdle) delay(5L)
            }
        }
    }
}
