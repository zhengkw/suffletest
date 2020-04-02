package com.zhengkw.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName:FlowBean
 * @author: zhengkw
 * @description:
 * @date: 20/02/27上午 9:51
 * @version:1.0
 * @since: jdk 1.8
 */
public class FlowBean implements WritableComparable<FlowBean> {

    //上行
    private long upFlow;
    // 下行
    private long downFlow;
    // 总量
    private long totalFlow;

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(totalFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.totalFlow = in.readLong();
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public FlowBean() {
        super();
    }

    /**
     * @param o 比较器被调用时 keyin作为参数传递
     * @descrption:只比较总流量
     * @return: int
     * @date: 20/02/27 上午 10:01
     * @author: zhengkw
     */
    @Override
    public int compareTo(FlowBean o) {
        return this.totalFlow > o.getTotalFlow() ? -1 : 1;
    }

    @Override
    public String toString() {
        return
                upFlow + "\t" + downFlow + "\t" + totalFlow;
    }
}
