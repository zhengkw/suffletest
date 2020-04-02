package com.zhengkw.groupingcomparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: OrderGroupComparator
 * @author: zhengkw
 * @description: 在Reduce端利用groupingComparator
 * 将订单id相同的kv聚合成组，
 * 然后取第一个即是该订单中最贵商品
 * @date: 20/02/27下午 2:25
 * @version:1.0
 * @since: jdk 1.8
 */
public class OrderGroupComparator extends WritableComparator {
    private int result;

    public OrderGroupComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //将orderid升序排序
        OrderBean aOrderBean = (OrderBean) a;
        OrderBean bOrderBean = (OrderBean) b;
//按照orderid作为key分组只会输出每组的第一条数据
        if (aOrderBean.getOrderId() > bOrderBean.getOrderId()) {
            result = 1;
        } else if (aOrderBean.getOrderId() < bOrderBean.getOrderId()) {
            result = -1;
        } else {
            result = 0;
        }
        return result;
    }
}
