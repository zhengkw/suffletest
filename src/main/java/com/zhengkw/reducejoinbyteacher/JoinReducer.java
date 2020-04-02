package com.zhengkw.reducejoinbyteacher;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName:JoinReducer
 * @author: zhengkw
 * @description:
 * @date: 20/03/01下午 10:38
 * @version:1.0
 * @since: jdk 1.8
 */
public class JoinReducer extends Reducer<NullWritable, OrderBean, NullWritable, OrderBean> {

    ArrayList<OrderBean> beans = new ArrayList<>();

    private Map<String, String> pdData = new HashMap<>();

    /***
     * @descrption: 通过partitioner来分组
     * 每次调用一次reduce则处理通key的一组数据
     *  对数据进行分类！！！！！！
     * @param key  空值
     * @param values  OrderBean对象
     * @param context
     * @return: void
     * @date: 20/03/01 下午 10:39
     * @author: zhengkw
     */
    @Override
    protected void reduce(NullWritable key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        for (OrderBean bean : values) {
            //必须在里面创建 否则 Array里面存的全是一个对象值（最后一个的值）
            OrderBean orderBean = new OrderBean();

            //来自order文件的存到一个array中
            if (bean.getSource().contains("order")) {

      /*遍历获取所有的值，并把值封装到一个Array里面
        因为遍历时bean对象的地址值没有发生改变，只有内容变了，
        所以不能通过直接存bean来修改值。*/

                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                beans.add(orderBean);
            } else {
                pdData.put(bean.getpId(), bean.getpName());
            }
        }


    }

    /**
     * @param context
     * @descrption: 这里进行join操作 写出数据
     * @return: void
     * @date: 20/03/01 下午 10:41
     * @author: zhengkw
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //super.cleanup(context);

        //只需要处理orderData中的数据
        for (OrderBean orderBean : beans) {

            //字段替换
            orderBean.setpName(pdData.get(orderBean.getpId()));

            //写出
            context.write(NullWritable.get(), orderBean);
        }
    }
}
