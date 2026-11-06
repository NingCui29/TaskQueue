package com.cn.tasks

/**
 * 背压拒绝策略。
 *
 * 定义任务队列达到 [BackpressureConfig.maxCapacity] 上限后，对新入队任务的处理方式。
 *
 * | 策略 | 新任务 | 旧任务 | 调用方阻塞 | 适用场景 |
 * | --- | --- | --- | --- | --- |
 * | [THROW_EXCEPTION] | 拒绝 | 保留 | 否 | 入队失败必须被感知 |
 * | [DISCARD] | 丢弃 | 保留 | 否 | 任务可丢失，重旧轻新 |
 * | [DISCARD_OLDEST] | 入队 | 淘汰一个 | 否 | 重新轻旧，如 UI 刷新 |
 * | [CALLER_RUNS] | 就地执行 | 保留 | 是 | 不可丢任务，可容忍降速 |
 * | [BLOCK] | 等待入队 | 保留 | 是 | 严格保序且不可丢任务 |
 *
 * @see BackpressureConfig
 * @see SerialTaskQueue
 * @see ConcurrentTaskQueue
 */
enum class RejectionPolicy {

    /**
     * 抛出异常。
     *
     * 队列满时抛出 [TaskQueueFullException]，由调用者处理。
     * 适用于必须确保任务入队、且入队失败需立即感知的场景。
     */
    THROW_EXCEPTION,

    /**
     * 丢弃新任务。
     *
     * 静默丢弃新任务，不抛异常。仅触发 [BackpressureConfig.onRejected]。
     * 适用于任务非关键、可丢弃的场景。
     */
    DISCARD,

    /**
     * 丢弃最旧任务。
     *
     * 淘汰队列中优先级最低且最早入队的任务，再让新任务入队。
     * 触发 [BackpressureConfig.onDiscarded]。适用于希望保留最新任务的场景。
     */
    DISCARD_OLDEST,

    /**
     * 调用者执行。
     *
     * 在调用者所处线程直接执行任务，天然形成反向节流。
     *
     * 注意：会阻塞调用者线程直至任务完成，禁止在主线程使用。
     */
    CALLER_RUNS,

    /**
     * 阻塞等待。
     *
     * 阻塞调用者线程，直到队列出现空位再入队。
     *
     * 同队列任务执行或同步通知中重入且容量已满时，返回容量拒绝；便捷 enqueue 抛
     * [TaskQueueFullException]，避免等待自身释放容量。未满时仍正常接收。
     *
     * 注意：仅可用于后台线程，禁止在主线程使用。
     */
    BLOCK,
}

/**
 * 任务队列已满异常。
 *
 * 队列容量耗尽且策略为 [RejectionPolicy.THROW_EXCEPTION]，或 [RejectionPolicy.BLOCK]
 * 在同队列任务执行/同步通知中重入时，由便捷 enqueue 抛出。
 *
 * ## 相对旧 `com.cn.core.task.TaskQueueFullException` 的调整
 *
 * 基类由 `Exception` 改为 `RuntimeException`。原实现继承 `Exception`（Java 侧为
 * 受检异常），而它由 `enqueue` 在容量耗尽这一运行期条件下抛出，
 * 语义上属于非受检异常；改为 `RuntimeException` 后 Java 调用方无需强制 `catch`。
 * Kotlin 调用方不受影响。
 *
 * @param queueName 队列名称
 * @param queueSize 触发时的队列大小
 * @param maxCapacity 队列最大容量
 */
class TaskQueueFullException(
    val queueName: String,
    val queueSize: Int,
    val maxCapacity: Int,
) : RuntimeException(
    "Task queue '$queueName' is full. Current size: $queueSize, Max capacity: $maxCapacity",
)
