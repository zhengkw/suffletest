package com.zhengkw.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName:TopNReducer
 * @author: zhengkw
 * @description: 将数据整合到一个文件  期望输出是 tel   up  down   total  按照total降序
 * @date: 20/02/27上午 10:45
 * @version:1.0
 * @since: jdk 1.8
 */
public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }


}
