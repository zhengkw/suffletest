package com.zhengkw.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName:OrderBean
 * @author: zhengkw
 * @description:
 * @date: 20/02/27下午 1:51
 * @version:1.0
 * @since: jdk 1.8
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private int orderId;
    private String product_id;
    private double money;
    private int result = 0;

    public OrderBean() {
    }

    public String getProduct_id() {
        return product_id;
    }

    public double getMoney() {
        return money;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public void setMoney(double money) {
        this.money = money;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeUTF(product_id);
        out.writeDouble(money);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.product_id = in.readUTF();
        this.money = in.readDouble();
    }


    /**
     * @param o
     * @descrption: 比较对象的N个属性，则一般称作N次排序
     * 所有订单数据按照id升序排序，
     * 如果id相同再按照金额降序排序，发送到Reduce
     * @return: int
     * @date: 20/02/27 下午 1:57
     * @author: zhengkw
     */
    @Override
    public int compareTo(OrderBean o) {
        //二次排序
        if (this.orderId > o.orderId) {
            result = 1;
        } else if (this.orderId < o.orderId) {
            result = -1;
        } else {
            //降序
            result = this.money > o.money ? -1 : 1;
        }


        return result;
    }

    @Override
    public String toString() {
        return orderId +
                "\t" + product_id +
                "\t" + money;
    }
}
