package com.cn.tasks

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class CancellationFailureAggregationTest {
    @Test fun queueCancellationReportsFatalFailuresRegardlessOfCallbackOrder() = runTest {
        for (concurrent in listOf(false, true)) {
            for (case in fatalCases()) verifyQueueCancellation(concurrent, case)
        }
    }

    @Test fun cancellationOnlyAndRepeatedInstancesRetainTheirExistingSemantics() = runTest {
        for (concurrent in listOf(false, true)) {
            for (case in repeatedAndCancellationCases()) verifyQueueCancellation(concurrent, case)
        }
    }

    @Test fun managerCancellationAndShutdownVisitEveryQueueBeforeReportingThePreferredFailure() = runTest {
        for (action in ManagerAction.entries) {
            for (case in fatalCases() + repeatedAndCancellationCases()) verifyManagerAction(action, case)
        }
    }

    private fun TestScope.verifyQueueCancellation(concurrent: Boolean, case: FailureCase) {
        val reported = mutableListOf<Throwable>()
        val dispatcher = StandardTestDispatcher(testScheduler)
        val owner = CoroutineScope(SupervisorJob() + dispatcher + CoroutineExceptionHandler { _, error ->
            reported += error
        })
        val serial = if (concurrent) null else SerialTaskQueue(owner, dispatcher)
        val parallel = if (concurrent) ConcurrentTaskQueue(owner, 1, dispatcher) else null
        val enqueue: (Task<Unit>) -> Task<Unit> = { serial?.enqueue(it) ?: parallel!!.enqueue(it) }
        var callbacks = 0
        fun cancelledTask() = observedTask { case.failures[callbacks++] }
        val first = cancelledTask()
        val second = cancelledTask()
        var escaped: Throwable? = null
        try {
            enqueue(first)
            enqueue(second)
            val canceller = owner.launch(start = CoroutineStart.UNDISPATCHED) {
                try {
                    serial?.cancelAll() ?: parallel!!.cancelAll()
                } catch (error: Throwable) {
                    escaped = error
                    throw error
                }
            }
            runCurrent()

            assertEquals(2, callbacks)
            assertSame(case.expected, escaped)
            assertEquals(case.suppressed, case.expected.suppressed.toList())
            assertEquals(case.reported, reported)
            assertTrue(canceller.isCancelled)
            assertEquals(TaskState.Cancelled, first.currentState)
            assertEquals(TaskState.Cancelled, second.currentState)
            assertTrue(serial?.isIdle ?: parallel!!.isIdle)
            assertTrue(owner.isActive)

            val replacement = enqueue(Task(id = first.id, block = { Unit }))
            runCurrent()
            assertEquals(TaskState.Completed(true), replacement.currentState)
            assertTrue(serial?.isIdle ?: parallel!!.isIdle)
            assertEquals(case.reported, reported)
        } finally {
            serial?.shutdown()
            parallel?.shutdown()
            owner.cancel()
            runCurrent()
        }
    }

    private fun TestScope.verifyManagerAction(action: ManagerAction, case: FailureCase) {
        val reported = mutableListOf<Throwable>()
        val dispatcher = StandardTestDispatcher(testScheduler)
        val owner = CoroutineScope(SupervisorJob() + dispatcher + CoroutineExceptionHandler { _, error ->
            reported += error
        })
        TaskManager.initialize(owner)
        val firstQueue = TaskManager.createSerialQueue("aggregation_first", dispatcher).apply { pause() }
        val secondQueue = TaskManager.createSerialQueue("aggregation_second", dispatcher).apply { pause() }
        var callbacks = 0
        val taskId = "shared_aggregation_id"
        // 按实际通知序号选择错误，不依赖管理器注册表的迭代顺序。
        val first = observedTask(taskId) { case.failures[callbacks++] }
        val second = observedTask(taskId) { case.failures[callbacks++] }
        var escaped: Throwable? = null
        try {
            firstQueue.enqueue(first)
            secondQueue.enqueue(second)
            val canceller = owner.launch(start = CoroutineStart.UNDISPATCHED) {
                try {
                    when (action) {
                        ManagerAction.CANCEL_ALL -> TaskManager.cancelAll()
                        ManagerAction.CANCEL_TASK -> TaskManager.cancelTask(taskId)
                        ManagerAction.SHUTDOWN -> TaskManager.shutdown()
                    }
                } catch (error: Throwable) {
                    escaped = error
                    throw error
                }
            }
            runCurrent()

            assertEquals(2, callbacks)
            assertSame(case.expected, escaped)
            assertEquals(case.suppressed, case.expected.suppressed.toList())
            assertEquals(case.reported, reported)
            assertTrue(canceller.isCancelled)
            assertEquals(TaskState.Cancelled, first.currentState)
            assertEquals(TaskState.Cancelled, second.currentState)
            assertEquals(0, firstQueue.pendingCount)
            assertEquals(0, secondQueue.pendingCount)
            assertTrue(owner.isActive)

            val resumedQueue = if (action == ManagerAction.SHUTDOWN) {
                assertTrue(firstQueue.isTerminated)
                assertTrue(secondQueue.isTerminated)
                assertTrue(TaskManager.getQueueStats().isEmpty())
                TaskManager.initialize(owner)
                TaskManager.createSerialQueue("aggregation_first", dispatcher)
            } else {
                assertFalse(TaskManager.cancelTask(taskId))
                firstQueue.apply { resume() }
            }
            val replacement = resumedQueue.enqueue(Task(id = taskId, block = { Unit }))
            runCurrent()
            assertEquals(TaskState.Completed(true), replacement.currentState)
            assertTrue(resumedQueue.isIdle)
            assertEquals(case.reported, reported)
        } finally {
            TaskManager.shutdown()
            owner.cancel()
            runCurrent()
        }
    }

    private fun observedTask(id: String? = null, failure: () -> Throwable): Task<Unit> {
        val stateCallback: (TaskState) -> Unit = { if (it is TaskState.Cancelled) throw failure() }
        return if (id == null) {
            Task(block = { fail("Cancelled queued work must not execute") }, onStateChange = stateCallback)
        } else {
            Task(id = id, block = { fail("Cancelled queued work must not execute") }, onStateChange = stateCallback)
        }
    }

    private fun fatalCases(): List<FailureCase> {
        val cancellationFirst = CancellationException("first cancellation")
        val fatalSecond = AssertionError("later fatal")
        val fatalFirst = LinkageError("first fatal")
        val cancellationSecond = CancellationException("later cancellation")
        val firstFatal = AssertionError("original fatal")
        val secondFatal = LinkageError("secondary fatal")
        val firstCancellation = CancellationException("first cancellation before throwable")
        val directThrowable = object : Throwable("non-exception failure") {}
        return listOf(
            FailureCase(listOf(cancellationFirst, fatalSecond), fatalSecond, listOf(cancellationFirst), listOf(fatalSecond)),
            FailureCase(listOf(fatalFirst, cancellationSecond), fatalFirst, listOf(cancellationSecond), listOf(fatalFirst)),
            FailureCase(listOf(firstFatal, secondFatal), firstFatal, listOf(secondFatal), listOf(firstFatal)),
            FailureCase(listOf(firstCancellation, directThrowable), directThrowable, listOf(firstCancellation), listOf(directThrowable)),
        )
    }

    private fun repeatedAndCancellationCases(): List<FailureCase> {
        val first = CancellationException("first cancellation")
        val second = CancellationException("second cancellation")
        val repeatedCancellation = CancellationException("same cancellation")
        val repeatedFatal = AssertionError("same fatal")
        return listOf(
            FailureCase(listOf(first, second), first, listOf(second), emptyList()),
            FailureCase(listOf(repeatedCancellation, repeatedCancellation), repeatedCancellation, emptyList(), emptyList()),
            FailureCase(listOf(repeatedFatal, repeatedFatal), repeatedFatal, emptyList(), listOf(repeatedFatal)),
        )
    }

    private data class FailureCase(
        val failures: List<Throwable>,
        val expected: Throwable,
        val suppressed: List<Throwable>,
        val reported: List<Throwable>,
    )

    private enum class ManagerAction { CANCEL_ALL, CANCEL_TASK, SHUTDOWN }
}
