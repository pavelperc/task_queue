import java.util.*

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
        println("task $id started working.")
        Thread.sleep(workTime.toLong())
        println("task $id finished working.")
        
    }
    
    override fun toString() = "Task $id: priority = $priority, workTime = $workTime"
}


/** This is a container with tasks. It extends thread and runs tasks in the priority order.*/
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
    
    
    private val queue = PriorityQueue<Task> { task1, task2 ->
        // firstly compare priorities
        var ans = task1.priority.compareTo(task2.priority)
        // secondly compare working time
        if (ans == 0)
            ans = task1.workTime.compareTo(task2.workTime)
        
        ans
    }
    
    
    /** The sum of all [Task.workTime] among all tasks in [queue]*/
    var filledSpace = 0
        private set
    
    fun canAddTask(task: Task) = filledSpace + task.workTime <= timeCapacity
    
    
    fun addTask(task: Task) {
        if (canAddTask(task)) {
            queue.add(task)
            filledSpace += task.workTime
            println("successfully pushed task ${task.id} to queue $id")
        } else {
            throw Exception("Tried to add task ${task.id}, which doesn't fit to queue $id")
        }
    }
    
    override fun run() {
        while (true) {// active waiting
            // trying to get the shortest task with the highest priority
            val task = queue.peek()
            
            Thread.sleep(100)
            
            if (task != null) {
                task.work()
                filledSpace -= task.workTime
                queue.remove()
            }
        }
    }
    
    override fun toString(): String {
        val taskIds = queue.map { task -> task.id }.joinToString(", ")
        return "Queue $id: filled: ($filledSpace from $timeCapacity), tasks = [${queue.joinToString("; ")}]"
    }
    
    fun isEmpty() = queue.isEmpty()
}

class QueueHandler(val queuesNumber: Int) {
    
    private val queues = List(queuesNumber) { TaskQueue() }
    
    init {
        queues.forEach { it.start() }
    }
    
    fun addTask(task: Task) {
        println("trying to push $task")
        println(this)
        println()
        var bestQueue: TaskQueue?
        do {
            bestQueue = queues
                .filter { it.canAddTask(task) }// take only queues where we can put the task
                .minBy { it.timeCapacity - it.filledSpace }// choose one with the least left space
        } while (bestQueue == null)// active waiting
        
        bestQueue.addTask(task)
        println()
    }
    
    override fun toString(): String {
        return "All queues: ${queues.joinToString("\n\t", "\n\t")}"
    }
    
    /** Waits until all queues become empty*/
    fun waitAllTasks() {
        while (!queues.all { it.isEmpty() }) {
//            Thread.sleep(100)
//            println("waiting queues: $this")
        }
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
    
    queueHandler.waitAllTasks()
    
}