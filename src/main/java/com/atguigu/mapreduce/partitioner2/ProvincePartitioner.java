package com.atguigu.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Gzc
 * @version 1.0
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
	@Override
	public int getPartition(Text text, FlowBean flowBean, int i) {

		// text 是手机号
		String phone = text.toString();

		// 根据手机号返回分区
		int	partition;

		String prePhone = phone.substring(0, 3);
		if ("136".equals(prePhone)){
			partition = 0;
		}else if ("137".equals(prePhone)){
			partition = 1;
		}else if ("138".equals(prePhone)){
			partition = 2;
		}else if ("139".equals(prePhone)){
			partition = 3;
		}else {
			partition = 4;
		}

		return partition;
	}
}
