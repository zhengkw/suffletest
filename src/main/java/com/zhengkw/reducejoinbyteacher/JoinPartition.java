package com.zhengkw.reducejoinbyteacher;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName:JoinPatition
 * @author: zhengkw
 * @description:
 * @date: 20/03/01下午 10:23
 * @version:1.0
 * @since: jdk 1.8
 */
public class JoinPartition extends Partitioner<NullWritable, OrderBean> {
    /**
     * @param nullWritable
     * @param orderBean
     * @param numPartitions
     * @descrption: 通过分区将数据分开
     * @return: int
     * @date: 20/03/01 下午 10:37
     * @author: zhengkw
     */
    @Override
    public int getPartition(NullWritable nullWritable, OrderBean orderBean, int numPartitions) {
        return orderBean.getpId().hashCode() % numPartitions;
    }
}
