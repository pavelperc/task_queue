import io.kotlintest.Duration
import io.kotlintest.matchers.beEmpty
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.StringSpec
import sun.invoke.empty.Empty
import java.util.concurrent.TimeUnit

/**
 * Created by pavel on 01.05.2018.
 */


/** After working it adds its [id] to [idList]*/
class TestTask(workTime: Int, priority: Int, id: Int, private val idList: MutableList<Int>) :
    Task(workTime, priority, id) {
    
    override fun work() {
        super.work()
        idList += id
    }
}


class PriorityTest : StringSpec() {
    init {
        
        "priority should be respected" {
            val invokeOrder = mutableListOf<Int>()
            
            val task1 = TestTask(100, 1, 1, invokeOrder)
            val task2 = TestTask(100, 2, 2, invokeOrder)
            val task3 = TestTask(100, 3, 3, invokeOrder)
            
            val queue = TaskQueue(300)
            
            queue.addTask(task3)
            queue.addTask(task1)
            queue.addTask(task2)
            
            queue.start()
            queue.interrupt()// finishes all tasks
            queue.join()
            
            
            invokeOrder shouldBe listOf(1, 2, 3)
        }
        
        "tasks with the same priority should be sorted by working time" {
            //            val queueHandler = QueueHandler(1, 3000)
            
            val invokeOrder = mutableListOf<Int>()
            
            val task1 = TestTask(100, 1, 1, invokeOrder)
            val task2 = TestTask(10, 1, 2, invokeOrder)
            val task3 = TestTask(100, 2, 3, invokeOrder)
            val task4 = TestTask(200, 2, 4, invokeOrder)
            
            val queue = TaskQueue(1000)
            
            queue.addTask(task4)
            queue.addTask(task3)
            queue.addTask(task2)
            queue.addTask(task1)
            
            queue.start()
            queue.interrupt()// finishes all tasks
            queue.join()
            
            
            invokeOrder shouldBe listOf(2, 1, 3, 4)
        }
    }
}


class FilledQueueTest : StringSpec() {
    init {
        "Filled queue should throw Exception when adding" {
            val queue = TaskQueue(300)
            val task1 = Task(100)
            val task2 = Task(200)
            val task3 = Task(500)
            
            queue.addTask(task1)
            queue.addTask(task2)
            
            shouldThrow<IllegalStateException> {
                queue.addTask(task3)
            }
        }
        
        
        "canAdd should check if there is enough space in the queue." {
            val queue = TaskQueue(300)
            val task1 = Task(100)
            val task2 = Task(100)
            val task3 = Task(500)
            
            queue.addTask(task1)
            
            queue.canAddTask(task2) shouldBe true
            queue.addTask(task2)
            
            queue.canAddTask(task1) shouldBe true
            queue.canAddTask(task3) shouldBe false
        }
    }
}

class TestQueue(timeCapacity: Int, id: Int) : TaskQueue(timeCapacity, id) {
    /** @return set of working and waiting tasks in the queue.
     * May be unstable because of [workingTask]*/
    val taskIds: Set<Int>
        get() {
            val ids = queue.map { it.id }.toMutableSet()
            
            val workingId = workingTask?.id
            
            if (workingId != null)
                ids.add(workingId)

//            println("return taskIds: $ids for queue $this")
            return ids
        }
}

class QueueHandlerTest : StringSpec() {
    init {
        "handler should find unfilled queue with the least free space." {
            val queue1 = TestQueue(500, 1)
            val queue2 = TestQueue(300, 2)
            
            val task1 = Task(200, 1, 1)
            val task2 = Task(100, 2, 2)
            val task3 = Task(500, 3, 3)
            
            val handler = QueueHandler(listOf(queue1, queue2))
            
            handler.addTask(task1)
            handler.addTask(task2)
            
            // all tasks should be in queue 2, because it has less free space.
            // we reserve space in queue1 for some big tasks
            
            queue1.taskIds shouldBe setOf<Int>()
            queue2.taskIds shouldBe setOf(1, 2)
            
            // add big task to queue1 without waiting
            handler.addTask(task3)
            queue1.taskIds shouldBe setOf(3)
            
            handler.shutdownAllTasks()
        }
        
        "handler should wait first freed queue." {
            val queue1 = TestQueue(100, 1)
            val queue2 = TestQueue(100, 2)
            
            val task1 = Task(100, 1, 1)
            val task2 = Task(50, 2, 2)
            val task3 = Task(100, 3, 3)
            
            // manually fill queues
            queue1.addTask(task1)
            queue2.addTask(task2)
            
            val handler = QueueHandler(listOf(queue1, queue2))
            
            // should wait 50 ms to free task2
            
            handler.addTask(task3)
            queue2.taskIds shouldBe setOf(3)
        }.config(timeout = Duration(1000, TimeUnit.MILLISECONDS))
    }
}


class ShutdownTest1 : StringSpec() {
    init {
        "shutdown while execute some tasks." {
            val tasks = List(6) { i -> Task(50, 1, i) }
            
            val queue1 = TestQueue(150, 1)
            val queue2 = TestQueue(150, 2)
            
            val handler = QueueHandler(listOf(queue1, queue2))
            
            tasks.forEach { handler.addTask(it) }
            
            Thread.sleep(55)
            
            queue1.taskIds.size + queue2.taskIds.size shouldBe 4
            
            handler.shutdownAllTasks()
            
            queue1.taskIds.size + queue2.taskIds.size shouldBe 0
        }.config(5, timeout = Duration(1000, TimeUnit.MILLISECONDS))
    }
}

class ShutdownTest2 : StringSpec() {
    init {
        "shutdown while changing working task." {
            val tasks = List(6) { i -> Task(50, 1, i) }
        
            val queue1 = TestQueue(150, 1)
            val queue2 = TestQueue(150, 2)
        
            val handler = QueueHandler(listOf(queue1, queue2))
        
            tasks.forEach { handler.addTask(it) }
        
            Thread.sleep(100)
            // now we are changing working task
            handler.shutdownAllTasks()
        
            val ids1 = queue1.taskIds
            val ids2 = queue2.taskIds
        
            ids1 should beEmpty()
            ids2 should beEmpty()
        }.config(invocations = 20, timeout = Duration(5000, TimeUnit.MILLISECONDS))
    }
}

class ShutdownTest3 : StringSpec() {
    init {
        "shutdown on empty queues." {
            val tasks = List(6) { i -> Task(50, 1, i) }
            
            val queue1 = TestQueue(100, 1)
            val queue2 = TestQueue(100, 2)
            val queue3 = TestQueue(100, 3)
            
            val handler = QueueHandler(listOf(queue1, queue2, queue3))
            
            tasks.forEach { handler.addTask(it) }
            
            Thread.sleep(150)
            // now all queues are empty
            
            queue1.taskIds should beEmpty()
            queue2.taskIds should beEmpty()
            queue3.taskIds should beEmpty()
            
            // just stop queues waiting new Tasks
            handler.shutdownAllTasks()
            
            queue1.taskIds should beEmpty()
            queue2.taskIds should beEmpty()
            queue3.taskIds should beEmpty()
            
        }.config(invocations = 10, timeout = Duration(3000, TimeUnit.MILLISECONDS))
    }
}




