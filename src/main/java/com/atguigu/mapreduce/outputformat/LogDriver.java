package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class LogDriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(LogDriver.class);

		job.setMapperClass(LogMapper.class);
		job.setReducerClass(LogReducer.class);

		job.setMapOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		//设置自定义outputFormat
		job.setOutputFormatClass(LogOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("G:\\hadoop\\input\\inputoutputformat"));
		//虽然我]自定义了outputformat,但是因为我们outputformat.继承自Fileoutputformat
		// 而耐ileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
		FileOutputFormat.setOutputPath(job, new Path("G:\\hadoop\\output999"));

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
