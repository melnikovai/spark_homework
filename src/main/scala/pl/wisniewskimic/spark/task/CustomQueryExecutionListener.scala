package pl.wisniewskimic.spark.task

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import java.time.LocalDateTime
import org.json4s.native.Serialization.write

class CustomQueryExecutionListener(appName: String) extends SparkListener {

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskMetrics = taskEnd.taskMetrics
    val taskInfo = taskEnd.taskInfo

    val now = LocalDateTime.now()
    val executor = taskInfo.executorId
    val taskid = taskInfo.taskId
    val status = taskInfo.status
    val cpuTime = taskMetrics.executorCpuTime / 1000000L
    val writtenrows = taskMetrics.outputMetrics.recordsWritten
    val peakmemory = taskMetrics.peakExecutionMemory

    val metrics = CustomTaskMetrics(appName, now, executor, taskid, status, cpuTime, writtenrows, peakmemory )
    sendMetrics(metrics)
  }

  def sendMetrics(metrics: CustomTaskMetrics): Unit ={
    import FormatterUtils.formats
    println( write(metrics) )
  }

}

case class CustomTaskMetrics(appname: String,
                             datetime: LocalDateTime,
                             executor: String,
                             taskid: Long,
                             status: String,
                             cputime: Long,
                             writtenrows: Long,
                             memorypeak: Long)
