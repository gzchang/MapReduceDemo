package com.atguigu.mapreduce.combineTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class WordCountDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// 1 获取job
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 设置jar包路径
		job.setJarByClass(WordCountDriver.class);

		// 3 关联mapper和reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// 4 设置map输出的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 设置最终输出的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 如果不设置InputFormat，他默认用得是TextInputFormat.class
		job.setInputFormatClass(CombineTextInputFormat.class);
		// 虚拟存储切片最大值设置为4MB
		CombineTextInputFormat.setMaxInputSplitSize(job,4*1024*1024);
		// 此时切片数目应该为3片

		// 6 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path("G:\\hadoop\\input\\inputcombinetextinputformat"));
		FileOutputFormat.setOutputPath(job, new Path("G:\\hadoop\\outputCombine2"));

		// 7 提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
