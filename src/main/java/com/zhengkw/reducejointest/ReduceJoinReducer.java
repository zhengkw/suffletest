package com.zhengkw.reducejointest;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 1.在reduce()方法中,key和遍历迭代器中的value，在同一个Reducer对象中，一直都是同一个对象
 * 
 * 		每次遍历时，key和value中的属性会随之变化！
 */
public class ReduceJoinReducer extends Reducer<NullWritable, OrderBean,NullWritable , OrderBean>{

	private List<OrderBean> orderData=new ArrayList<>();
	
	private Map<String, String> pdData=new HashMap<>();
	
	private NullWritable valueOut=NullWritable.get();
	
	//分类
	@Override
	protected void reduce(NullWritable key, Iterable<OrderBean> value,
			Context context)
			throws IOException, InterruptedException {
		
		// orderBean作为形参
		for (OrderBean orderBean : value) {
			
			if (orderBean.getSource().contains("order")) {
				
				OrderBean o = new OrderBean();
				
				try {
					BeanUtils.copyProperties(o, orderBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				
				orderData.add(o);
				
			}else {
				
				//pid为key,pname为value
				pdData.put(orderBean.getPid(), orderBean.getPname());
				
			}
			
		}
		
		
	}
	
	//真正Join
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		//只需要处理orderData中的数据
		for (OrderBean orderBean : orderData) {
			
			//字段替换
			orderBean.setPname(pdData.get(orderBean.getPid()));

			//写出
			context.write(valueOut ,orderBean);
		}
		
	}
}
