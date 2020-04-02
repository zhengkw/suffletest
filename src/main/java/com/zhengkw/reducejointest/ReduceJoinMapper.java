package com.zhengkw.reducejointest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class ReduceJoinMapper extends Mapper<LongWritable, Text, NullWritable, OrderBean>{

	
	private NullWritable keyOut=NullWritable.get();
	
	private OrderBean valueOut=new OrderBean();
	
	//记录当前切片来自哪个文件
	private String source;
	
	//在map()之前运行，且只运行一次
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		FileSplit inputSplit = (FileSplit)context.getInputSplit();
		
		source=inputSplit.getPath().getName();
		
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		//打标记
		valueOut.setSource(source);
		
		String[] words = value.toString().split("\t");
		
		//数据来源自order.txt
		if (source.contains("order.txt")) {
			
			valueOut.setOrderId(words[0]);
			valueOut.setPid(words[1]);
			valueOut.setAmount(Integer.parseInt(words[2]));
			
			valueOut.setPname("nodata");
			
		}else {
			valueOut.setOrderId("nodata");
			valueOut.setPid(words[0]);
			valueOut.setAmount(0);
			
			valueOut.setPname(words[1]);
			
			
		}
		
		context.write(keyOut, valueOut);
		
		 
		
	}
}
