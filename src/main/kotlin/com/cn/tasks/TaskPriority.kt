package com.cn.tasks

/**
 * 任务优先级。
 *
 * 队列按「优先级降序 + 入队序号升序」排序，即：
 *  1. 优先级高的先执行；
 *  2. 同优先级按 FIFO 执行。
 *
 * 枚举天然实现 [Comparable]（按 `ordinal` 比较），因此声明顺序必须保持
 * `LOW < NORMAL < HIGH < IMMEDIATE`——调度器直接使用枚举自身比较，
 * 不再依赖旧实现里手写的 `it.priority.ordinal`。
 *
 * @see Task
 * @see TaskQueue
 */
enum class TaskPriority {
    /** 低优先级：后台预热、日志上报等可延后的工作。 */
    LOW,

    /** 普通优先级：默认值，适用于绝大多数常规任务。 */
    NORMAL,

    /** 高优先级：用户可感知、需要快速响应的工作。 */
    HIGH,

    /** 最高优先级：必须尽快执行的紧急任务。 */
    IMMEDIATE,
}
