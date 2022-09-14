package com.atguigu.mapreduce.reduecJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author Gzc
 * @version 1.0
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {

		//01 1001 1 order
		//01 1004 4 order
		//01 小米    pd

		//准备初始化集合
		ArrayList<TableBean> orderBeans = new ArrayList<>();
		TableBean pdBean = new TableBean();

		//循环遍历
		for (TableBean value : values) {
			if ("order".equals(value.getFlag())){
				//订单表//直接添加，给的是地址
				//要复制对象
				TableBean tempTableBean = new TableBean();

				try {
					BeanUtils.copyProperties(tempTableBean,value);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				orderBeans.add(tempTableBean);
			}else {
				//pd表进来的只能是一行
				try {
					BeanUtils.copyProperties(pdBean,value);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}

			}
		}
		//循环遍历orderBeans，赋值pdName
		for (TableBean orderBean : orderBeans) {
			orderBean.setpName(pdBean.getpName());
			context.write(orderBean,NullWritable.get());
		}
	}
}
