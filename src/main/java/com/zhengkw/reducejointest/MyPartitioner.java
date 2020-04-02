package com.zhengkw.reducejointest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

//pid相同的分到同一个区
public class MyPartitioner extends Partitioner<NullWritable, OrderBean>{

	@Override
	public int getPartition(NullWritable key, OrderBean value, int numPartitions) {
		
		return value.getPid().hashCode() % numPartitions;
	}

}
