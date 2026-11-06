package com.cn.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.withContext
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class TaskManagerTest {
    @Test fun pausedSerialStatsIncludeTheTaskUntilCancellationCleanupFinishes() = runTest {
        TaskManager.initialize(this)
        val releaseCleanup = CompletableDeferred<Unit>()
        var cleaning = false
        val queue = TaskManager.createSerialQueue("paused_cleanup", StandardTestDispatcher(testScheduler))
        queue.enqueue(Task<Unit>(block = {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    cleaning = true
                    releaseCleanup.await()
                }
            }
        }))
        try {
            runCurrent()
            assertEquals(1, TaskManager.getQueueStats().getValue("serial_paused_cleanup").runningCount)

            queue.pause()
            runCurrent()
            assertTrue(cleaning)
            assertFalse(queue.isIdle)
            assertEquals(SerialTaskQueue.QueueState.Paused(0), queue.state.value)
            assertEquals(1, queue.runningTaskCount)
            assertEquals(1, TaskManager.getQueueStats().getValue("serial_paused_cleanup").runningCount)

            releaseCleanup.complete(Unit)
            runCurrent()
            assertTrue(queue.isIdle)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, TaskManager.getQueueStats().getValue("serial_paused_cleanup").runningCount)
        } finally {
            releaseCleanup.complete(Unit)
            runCurrent()
            TaskManager.shutdown()
            runCurrent()
        }
    }

    @Test fun closingSerialStatsIncludeTheTaskUntilShutdownCleanupFinishes() = runTest {
        TaskManager.initialize(this)
        val releaseCleanup = CompletableDeferred<Unit>()
        var cleaning = false
        val queue = TaskManager.createSerialQueue("closing_cleanup", StandardTestDispatcher(testScheduler))
        queue.enqueue(Task<Unit>(block = {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    cleaning = true
                    releaseCleanup.await()
                }
            }
        }))
        try {
            runCurrent()
            queue.shutdown()
            runCurrent()
            assertTrue(cleaning)
            assertFalse(queue.isTerminated)
            assertEquals(SerialTaskQueue.QueueState.Closing(1), queue.state.value)
            assertEquals(1, queue.runningTaskCount)
            assertEquals(1, TaskManager.getQueueStats().getValue("serial_closing_cleanup").runningCount)

            releaseCleanup.complete(Unit)
            runCurrent()
            assertTrue(queue.isTerminated)
            assertEquals(SerialTaskQueue.QueueState.Shutdown, queue.state.value)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, TaskManager.getQueueStats().getValue("serial_closing_cleanup").runningCount)
            assertTrue(isActive)
        } finally {
            releaseCleanup.complete(Unit)
            runCurrent()
            TaskManager.shutdown()
            runCurrent()
        }
    }

    @Test fun shutdownClosesOldHandlesAndRequiresExplicitReinitialization() = runTest {
        TaskManager.initialize(this)
        try {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val serial = TaskManager.createSerialQueue("owned_serial", dispatcher)
            val concurrent = TaskManager.createConcurrentQueue("owned_concurrent", 1, dispatcher)
            val pending = Task<Unit>(block = { fail("Shutdown must cancel pending tasks") })
            serial.enqueue(pending)
            TaskManager.shutdown()
            assertTrue(serial.isClosed)
            assertTrue(concurrent.isClosed)
            assertEquals(TaskState.Cancelled, pending.currentState)
            val rejected = Task<Unit>(block = { fail("Manager must reject after shutdown") })
            try {
                TaskManager.submitSerial(rejected)
                fail("Closed manager must not recreate its default queue")
            } catch (_: TaskQueueClosedException) { }
            assertEquals(TaskState.Cancelled, rejected.currentState)
            runCurrent()
            assertTrue(serial.isTerminated)
            assertTrue(concurrent.isTerminated)
            assertTrue(isActive)

            TaskManager.initialize(this)
            var completed = false
            TaskManager.createSerialQueue("default_serial", dispatcher)
            TaskManager.submitSerial(Task(block = { completed = true }))
            runCurrent()
            assertTrue(completed)
        } finally {
            TaskManager.shutdown()
            runCurrent()
        }
    }

    @Test fun removedDefaultQueuesAreRegisteredAgainBeforeFurtherSubmissions() = runTest {
        TaskManager.initialize(this)
        try {
            // First submission must resolve the default through the manager's fallback path.
            TaskManager.submitSerial(Task(block = { Unit }))
            TaskManager.submitConcurrent(Task(block = { Unit }))
            val oldSerial = TaskManager.getSerialQueue("default_serial")
            val oldConcurrent = TaskManager.getConcurrentQueue("default_concurrent")
            assertNotNull(oldSerial)
            assertNotNull(oldConcurrent)

            TaskManager.removeQueue("default_serial")
            TaskManager.removeQueue("default_concurrent")
            assertTrue(oldSerial!!.isClosed)
            assertTrue(oldConcurrent!!.isClosed)
            val serial = Task<Unit>(block = { fail("Manager cancellation must still own the task") })
            val concurrent = Task<Unit>(block = { fail("Manager cancellation must still own the task") })
            TaskManager.submitSerial(serial)
            TaskManager.submitConcurrent(concurrent)

            assertNotNull(TaskManager.getSerialQueue("default_serial"))
            assertNotNull(TaskManager.getConcurrentQueue("default_concurrent"))
            assertNotSame(oldSerial, TaskManager.getSerialQueue("default_serial"))
            assertNotSame(oldConcurrent, TaskManager.getConcurrentQueue("default_concurrent"))
            TaskManager.cancelAll()
            assertEquals(TaskState.Cancelled, serial.currentState)
            assertEquals(TaskState.Cancelled, concurrent.currentState)
            assertTrue(isActive)
        } finally {
            TaskManager.shutdown()
            runCurrent()
        }
    }
}
