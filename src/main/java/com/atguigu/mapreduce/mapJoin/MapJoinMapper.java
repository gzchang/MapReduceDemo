package com.atguigu.mapreduce.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author Gzc
 * @version 1.0
 */
public class MapJoinMapper extends Mapper<LongWritable, Text,Text,NullWritable> {

	private HashMap<String,String> pdMap = new HashMap<>();
	private Text outK = new Text();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		//获取缓存的文件，并把文件内容封装到集合 pd.txt
		URI[] cacheFiles = context.getCacheFiles();

		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		FSDataInputStream fis = fileSystem.open(new Path(cacheFiles[0]));

		//从流中读出数据
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
		String line;

		//导common包的
		while (StringUtils.isNotEmpty(line = reader.readLine())){
			String[] fields = line.split("\t");
			pdMap.put(fields[0],fields[1]);
		}
		//灌流导Hadoop包的
		IOUtils.closeStream(reader);
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

		// 处理order.txt
		String line = value.toString();
		String[] fields = line.split("\t");

		//获取pid
		String pid = fields[1];
		String pName = pdMap.get(pid);

		//获取订单id和订单数量
		outK.set(fields[0]+"\t"+pName+"\t"+fields[2]);

		context.write(outK, NullWritable.get());
	}
}
