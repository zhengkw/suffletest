package com.zhengkw.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName:OrderReducer
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 2:12
 * @version:1.0
 * @since: jdk 1.8
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, NullWritable,OrderBean> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(),key);
    }
}