package com.cn.tasks

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueStateFlowTest {
    @Test fun ownershipWaitersWakeEvenWhenPublicStateRemainsEqual() = runTest {
        val state = QueueStateFlow("Idle")
        var idle = false
        val waiter = async(start = CoroutineStart.UNDISPATCHED) { state.awaitCondition { idle } }
        assertFalse(waiter.isCompleted)

        idle = true
        state.publish(QueueStateSnapshot(1L, "Idle"))
        waiter.await()

        assertTrue(waiter.isCompleted)
        assertEquals("Idle", state.value)
    }

    @Test fun staleSnapshotsCannotRestoreIdleOrClosingAndEqualStatesStayConflated() = runTest {
        val state = QueueStateFlow("Idle")
        val seen = mutableListOf<String>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            state.collect { seen += it }
        }

        state.publish(QueueStateSnapshot(2L, "Running"))
        state.publish(QueueStateSnapshot(1L, "Idle"))
        assertEquals("Running", state.value)
        state.publish(QueueStateSnapshot(3L, "Running"))
        state.publish(QueueStateSnapshot(5L, "Shutdown"))
        state.publish(QueueStateSnapshot(4L, "Closing"))

        assertEquals("Shutdown", state.value)
        assertEquals(listOf("Shutdown"), state.replayCache)
        assertEquals(listOf("Idle", "Running", "Shutdown"), seen)
    }

    @Test fun synchronousCollectorsCanCancelEachOthersQueueWithoutPublicationLocks() {
        val failures = ConcurrentLinkedQueue<Throwable>()
        val owner = CoroutineScope(SupervisorJob() + Dispatchers.Unconfined +
            CoroutineExceptionHandler { _, error -> failures.add(error) })
        val first = SerialTaskQueue(owner)
        val second = SerialTaskQueue(owner)
        val entered = CountDownLatch(2)
        val firstCollector = owner.launch {
            first.state.collect { state ->
                if (state is SerialTaskQueue.QueueState.Paused) {
                    entered.countDown()
                    check(entered.await(5, TimeUnit.SECONDS))
                    second.cancelAll()
                }
            }
        }
        val secondCollector = owner.launch {
            second.state.collect { state ->
                if (state is SerialTaskQueue.QueueState.Paused) {
                    entered.countDown()
                    check(entered.await(5, TimeUnit.SECONDS))
                    first.cancelAll()
                }
            }
        }
        val firstPause = FutureTask { first.pause() }
        val secondPause = FutureTask { second.pause() }
        // If publication locks regress, the timeout must fail without leaving non-daemon threads.
        val firstThread = Thread(firstPause, "first-queue-pause").apply { isDaemon = true }
        val secondThread = Thread(secondPause, "second-queue-pause").apply { isDaemon = true }
        try {
            firstThread.start()
            secondThread.start()
            firstPause.get(5, TimeUnit.SECONDS)
            secondPause.get(5, TimeUnit.SECONDS)
            assertTrue(failures.toString(), failures.isEmpty())
            assertEquals(SerialTaskQueue.QueueState.Paused(0), first.state.value)
            assertEquals(SerialTaskQueue.QueueState.Paused(0), second.state.value)
        } finally {
            firstCollector.cancel()
            secondCollector.cancel()
            firstThread.join(1_000)
            secondThread.join(1_000)
            // Closing a deadlocked queue would hide the bounded failure above.
            if (!firstThread.isAlive && !secondThread.isAlive) {
                first.shutdown()
                second.shutdown()
                owner.cancel()
            }
        }
    }
}
