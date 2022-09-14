package com.atguigu.mapreduce.ETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class WebLogDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		//输入输出路径
		args = new String[]{"G:\\hadoop\\input\\inputlog", "G:\\hadoop\\output123"};

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(WebLogDriver.class);

		job.setMapperClass(WebLogMapper.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setOutputValueClass(NullWritable.class);

		job.setOutputValueClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		//设置reduceTask个数为0
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);


	}
}
