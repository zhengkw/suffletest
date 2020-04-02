package com.zhengkw.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName:OrderMapper
 * @author: zhengkw
 * @description: （1）利用“订单id和成交金额”作为key，
 * 可以将Map阶段读取到的所有订单数据按照id升序排序，
 * 如果id相同再按照金额降序排序，发送到Reduce。
 * （2）在Reduce端利用groupingComparator
 * 将订单id相同的kv聚合成组，
 * 然后取第一个即是该订单中最贵商品
 * @date: 20/02/27下午 1:48
 * @version:1.0
 * @since: jdk 1.8
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    NullWritable v = NullWritable.get();
    OrderBean k = new OrderBean();


    /**
     * @param key     偏移量
     * @param value   一行数据
     * @param context 上下文
     * @descrption:处理数据封装
     * @return: void
     * @date: 20/02/27 下午 2:15
     * @author: zhengkw
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //10000001	Pdt_02	222.8
        String line = value.toString();
        String[] words = line.trim().split("\t");

        //orderid
        k.setOrderId(Integer.parseInt(words[0]));
        //pid
        k.setProduct_id(words[1]);
        //money
        k.setMoney(Double.parseDouble(words[2]));
        context.write(k, v);
    }
}
