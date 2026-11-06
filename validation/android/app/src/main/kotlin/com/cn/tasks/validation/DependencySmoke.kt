package com.cn.tasks.validation
import android.app.Activity
import android.os.Bundle
import com.cn.tasks.SerialTaskQueue
import com.cn.tasks.Task
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
class DependencySmoke : Activity() {
    private val owner = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        SerialTaskQueue(owner).enqueue(Task(block = { 42 }))
    }
    override fun onDestroy() { owner.cancel(); super.onDestroy() }
}
