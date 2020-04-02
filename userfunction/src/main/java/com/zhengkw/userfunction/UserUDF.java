package com.zhengkw.userfunction;


import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ClassName:UserUDF
 * @author: zhengkw
 * @description: 自定义 udf
 * @date: 20/03/11上午 12:26
 * @version:1.0
 * @since: jdk 1.8
 */
public class UserUDF extends UDF {

    /**
     * @param s
     * @descrption: 方法名只能是evaluate！！！！！
     * @return: 返回类型不能为void，可以为 null
     * @date: 20/03/11 上午 1:06
     * @author: zhengkw
     */
    public String evaluate(final String s) {

        if (s == null) {
            return null;
        }
         //返回一个小写字符串
        return s.toLowerCase();
    }

}
