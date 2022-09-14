package com.atguigu.mapreduce.reduecJoin;

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
public class TableDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(TableDriver.class);

		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);

		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("G:\\hadoop\\input\\inputtable"));
		FileOutputFormat.setOutputPath(job, new Path("G:\\hadoop\\output7289"));

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);

	}
}
