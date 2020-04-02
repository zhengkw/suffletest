package com.zhengkw.reducejoinbyteacher;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName:OrederBean
 * @author: zhengkw
 * @description:
 * @date: 20/03/01下午 10:02
 * @version:1.0
 * @since: jdk 1.8
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private String pId;
    private String pName;
    private int amount;
    //标记
    private String source;

    public OrderBean() {
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSource() {
        return source;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getpId() {
        return pId;
    }

    public String getpName() {
        return pName;
    }

    public int getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return
                orderId + '\t' + pName + '\t' + amount;

    }

    @Override
    public int compareTo(OrderBean o) {
        return this.compareTo(o);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pId);
        out.writeUTF(pName);
        out.writeUTF(source);
        out.writeInt(amount);


    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        pId = in.readUTF();
        pName = in.readUTF();
        source = in.readUTF();
        amount = in.readInt();

    }
}
