package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

	private FSDataOutputStream atguiguLog;
	private FSDataOutputStream otherLog;

	public LogRecordWriter(TaskAttemptContext job) {
		// 创建两条流
		// HDFS客户端
		try {
			FileSystem fileSystem = FileSystem.get(job.getConfiguration());

			atguiguLog = fileSystem.create(new Path("G:\\hadoop\\atguigu.log"));

			otherLog = fileSystem.create(new Path("G:\\hadoop\\other.log"));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
		// 具体写
		String log = key.toString();
		if (log.contains("atguigu")){
			atguiguLog.writeBytes(log+"\n");
		}else {
			otherLog.writeBytes(log+"\n");
		}
	}

	@Override
	public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

		//关闭流
		IOUtils.closeStream(atguiguLog);
		IOUtils.closeStream(otherLog);
	}
}
