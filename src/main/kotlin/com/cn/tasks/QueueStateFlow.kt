package com.cn.tasks

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update

/** 在队列锁内生成版本，发布时不持有队列锁或额外发布锁。 */
internal data class QueueStateSnapshot<T>(val version: Long, val state: T)

/** 防止较旧快照覆盖新状态，同时允许收集器同步重入任意队列。 */
@OptIn(InternalCoroutinesApi::class)
internal class QueueStateFlow<T>(initialState: T) : StateFlow<T> {
    private val snapshots = MutableStateFlow(QueueStateSnapshot(0L, initialState))

    override val value: T get() = snapshots.value.state

    override val replayCache: List<T> get() = listOf(value)

    fun publish(snapshot: QueueStateSnapshot<T>) {
        snapshots.update { current -> if (snapshot.version > current.version) snapshot else current }
    }

    /** 按实际所有权等待条件成立；相等公开状态的新版本也会唤醒等待者。 */
    suspend fun awaitCondition(condition: () -> Boolean) {
        snapshots.first { condition() }
    }

    override suspend fun collect(collector: FlowCollector<T>): Nothing {
        // 版本变化仅用于内部排序；相等的公开状态仍遵循 StateFlow 的合并语义。
        snapshots.map { it.state }.distinctUntilChanged().collect(collector)
        error("Queue state flow never completes")
    }
}
