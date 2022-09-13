package com.atguigu.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 */
public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

	private FlowBean outV = new FlowBean();

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

		//1 遍历集合，累加值
		long totalup = 0;
		long totalDown = 0;
		for (FlowBean value : values) {
			totalup += value.getUpFlow();
			totalDown += value.getDownFlow();
		}

		//2 封装outK，outV
		outV.setUpFlow(totalup);
		outV.setDownFlow(totalDown);
		outV.setSumFlow();

		//3 写出
		context.write(key,outV);
	}
}
