package com.atguigu.mapreduce.reduecJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

	private String fileName;
	private Text outK = new Text();
	private TableBean outV = new TableBean();

	//优化手段，获取一次
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
		// 初始化 order pd
		FileSplit inputSplit = (FileSplit) context.getInputSplit();

		fileName = inputSplit.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

		String line = value.toString();

		if (fileName.contains("order")){
			//说明处理的是订单表
			String[] split = line.split("\t");
			outK.set(split[1]);
			outV.setId(split[0]);
			outV.setPid(split[1]);
			outV.setAmount(Integer.parseInt(split[2]));
			outV.setpName("");
			outV.setFlag("order");

		}else {
			//处理的是产品表
			String[] split = line.split("\t");
			outK.set(split[0]);
			outV.setId("");
			outV.setPid(split[0]);
			outV.setAmount(0);
			outV.setpName(split[1]);
			outV.setFlag("pd");
		}

		//写出
		context.write(outK,outV);
	}
}
