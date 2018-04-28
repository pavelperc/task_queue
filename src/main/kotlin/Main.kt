import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.locks.ReentrantLock

/**
 * Created by pavel on 25.04.2018.
 */


val rnd = Random()

class Task(
    /** Estimated working time in microseconds*/
    val workTime: Int = rnd.nextInt(5000),
    /** The lowest value means the highest priority*/
    val priority: Int = rnd.nextInt(3) + 1
) {
    companion object {
        private var counter = 0
    }
    
    val id = counter++
    
    fun work() {
        println("task $id started working for $workTime ms. (sleeping is work!)")
        Thread.sleep(workTime.toLong())
        println("task $id finished working.")
        
    }
    
    override fun toString() = "Task $id: priority = $priority, workTime = $workTime"
}


/**
 * This is a container for tasks. It extends thread and runs tasks in some order.
 *
 * The task becomes deleted from inner [queue] right before its invocation.
 * But [leftSpace] restores just after the work is done.
 * */
class TaskQueue(
    /** Maximal estimated time in milliseconds for all the tasks in this queue.*/
    val timeCapacity: Int = 5000
) : Thread() {
    
    companion object {
        private var counter = 0
    }
    
    val id = counter++
    
    
    init {
        super.setDaemon(true)
    }
    
    
    /** Blocking queue with tasks, sorted by priority and then by waiting time.*/
    private val queue = PriorityBlockingQueue<Task>(11) { task1, task2 ->
        // firstly compare priorities
        var ans = task1.priority.compareTo(task2.priority)
        // secondly compare working time
        if (ans == 0)
            ans = task1.workTime.compareTo(task2.workTime)
        
        ans
    }
    
    
    /** Equals [timeCapacity] minus total of all [Task.workTime] among all tasks in [queue]*/
    @Volatile
    var leftSpace = timeCapacity
        private set
    
    
    @Synchronized
    fun canAddTask(task: Task) = leftSpace >= task.workTime
    
    
    /** @throws Exception when [task] can not be added.*/
    @Synchronized
    fun addTask(task: Task) {
        if (canAddTask(task)) {
            queue.add(task)
            synchronized(leftSpace) {
                leftSpace -= task.workTime
            }
            println("successfully pushed task ${task.id} to queue $id")
        } else {
            throw Exception("Tried to add task ${task.id}, which doesn't fit to queue: $this")
        }
    }
    
    
    /** Task which is working at the moment*/
    @Volatile
    private var workingTask: Task? = null
    
    
    /** Updates [workingTask] and [leftSpace]*/
    @Synchronized
    private fun runTask(task: Task) {
        workingTask = task
        task.work()
        // return space back AFTER THE WORK
        synchronized(leftSpace) { leftSpace += task.workTime }
        workingTask = null
    }
    
    /** Lock for avoiding [InterruptedException] while a task is working.*/
    private val canInterrupt = ReentrantLock()
    
    override fun run() {
        try {
            while (true) {
                // trying to get the shortest task with the highest priority
                // blocks process while queue is empty
                val task = queue.take()
                
                canInterrupt.lock()
                runTask(task)
                canInterrupt.unlock()
                
            }
        } catch (e: InterruptedException) {
            // launch left processes from the queue
            
            while (!queue.isEmpty()) {
                val task = queue.poll()
                        ?: throw Exception("tried to get element from empty queue after interruption call")
                runTask(task)
            }
        }
    }
    
    /** Overridden interruption waits until one current task finishes its work
     * for throwing interruption between two working tasks or while the [queue] is waiting for elements*/
    override fun interrupt() {
        // wait until some task ends working
        canInterrupt.lock()
        super.interrupt()
    }
    
    override fun toString(): String {
//        val taskIds = queue.map { task -> task.id }.joinToString(", ")
        return "Queue $id: left $leftSpace from $timeCapacity, waiting tasks = [${queue.joinToString("; ")}], " +
                "working task = ($workingTask)"
    }
    
}

class QueueHandler(val queuesNumber: Int) {
    
    private val queues = List(queuesNumber) { TaskQueue() }
    
    init {
        queues.forEach { it.start() }
    }
    
    @Synchronized
    fun addTask(task: Task) {
        println("trying to push $task")
        println(this)
        println()
        
        var bestQueue: TaskQueue?
        
        do {// busy waiting
            bestQueue = queues
                .filter { it.canAddTask(task) }// take only queues where we can put the task
                .minBy { it.leftSpace }// choose one with the least left space
        } while (bestQueue == null)// busy waiting while there are no available queues
        
        bestQueue.addTask(task)
        println()
    }
    
    override fun toString(): String {
        return "All queues: ${queues.joinToString("\n\t", "\n\t")}"
    }
    
    /** Waits until all queues become empty*/
    fun shutdownAllTasks() {
        println("shutdownAllTasks called")
        queues.forEach { it.interrupt(); it.join() }
    }
}


fun main(args: Array<String>) {
    val tasks = List(7) { Task() }
    
    println("All tasks:${tasks.joinToString("\n\t", "\n\t")}")
    println()
    
    val queueHandler = QueueHandler(2)
    
    println(queueHandler)
    println()
    
    for (task in tasks) {
        Thread.sleep(500)
        queueHandler.addTask(task)
    }
    
    // Waits until all tasks finish their work
    queueHandler.shutdownAllTasks()
    
}