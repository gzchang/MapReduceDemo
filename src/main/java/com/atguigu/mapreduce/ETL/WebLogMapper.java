package com.atguigu.mapreduce.ETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

		//1 获取一行
		String line = value.toString();

		//2 切割（整合到下一步了）
		String[] split = line.split(" ");

		//3 ETL
		boolean result = passLog(line, context);
		if (!result) {
			return;//这一条被干掉
		}

		//4 写出
		context.write(value,NullWritable.get());
	}

	private boolean passLog(String line, Context context) {

		//切割
		String[] fields = line.split(" ");

		//判断日志的长度是否大于11
		if (fields.length > 11){
			return true;
		}else{
			return false;
		}

	}
}
