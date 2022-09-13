package com.atguigu.mapreduce.partitioner2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class FlowDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// 1 获取job
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 设置jar包
		job.setJarByClass(FlowDriver.class);

		// 3 关联mapper和reducer
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);

		// 4 设置mapper输出的key和value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 设置最后输出的Key和value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(5);//与分区数相等

		// 6 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path("G:\\hadoop\\input\\inputflow"));
		FileOutputFormat.setOutputPath(job, new Path("G:\\hadoop\\output5"));

		// 7 提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
