package com.lee.spark

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.mapred.SparkHadoopMapRedUtil

import scala.collection.mutable

class SmallFileMergeCommitProtocol(
                                    jobId: String,
                                    path: String,
                                    dynamicPartitionOverwrite: Boolean = false)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path,dynamicPartitionOverwrite)
  with Serializable with Logging{

  override def setupJob(jobContext: JobContext): Unit = {
    logInfo("SmallFileMerge:: setupJob" + jobContext.getJobName + " Job ID: " + jobId + " Job output path:" + path)

    super.setupJob(jobContext)
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    logInfo("SmallFileMerge:: setupTask" )
    super.setupTask(taskContext)
  }

  override def newTaskTempFile(
                                taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val filename = getFilename(taskContext, ext)
    val tempFile = super.newTaskTempFile(taskContext,dir,ext)
    logInfo("SmallFileMerge:: newTaskTempFile " + filename + ". TempFile: " + tempFile)
    return tempFile
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    logInfo("SmallFileMerge:: commitTask" )
    super.commitTask(taskContext)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    logInfo("SmallFileMerge:: commitJob")
    super.commitJob(jobContext,taskCommits)
  }

}
