package com.atguigu.mapreduce.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Gzc
 * @version 1.0
 * 1 定义类实现Writable接口
 * 2 重写序列化和反序列化方法
 * 3 重写一个空参构造
 * 4 重写toString
 */
public class FlowBean implements Writable {

	//上行流量
	private long upFlow;
	private long downFlow;
	private long sumFlow;

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	public void setSumFlow() {
		this.sumFlow = this.upFlow + this.downFlow;
	}

	public FlowBean() {
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {

		dataOutput.writeLong(upFlow);
		dataOutput.writeLong(downFlow);
		dataOutput.writeLong(sumFlow);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.upFlow = dataInput.readLong();
		this.downFlow = dataInput.readLong();
		this.sumFlow = dataInput.readLong();
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}
}
