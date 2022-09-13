package com.atguigu.mapreduce.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text outK = new Text();
	//在map阶段不进行聚合操作，设置为1就行
	private IntWritable outV = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		// 1.获取一行数据
		String line = value.toString();

		//2.切割数据
		String[] words = line.split(" ");

		//3.循环写出
		for (String word : words) {
			//封装outK
			outK.set(word);

			//写出
			context.write(outK, outV);
		}
	}
}
