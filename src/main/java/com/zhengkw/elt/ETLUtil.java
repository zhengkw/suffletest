package com.zhengkw.elt;

import org.junit.Test;

/**
 * @ClassName:ETLUtil
 * @author: zhengkw
 * @description:
 * @date: 20/03/09下午 9:00
 * @version:1.0
 * @since: jdk 1.8
 */
public class ETLUtil {
    public static String parse(String line) {
        //切割
      String[] words = line.split("\t");
        if (words.length < 10) {
            return null;
        }
        //将分类里 person & blog 中的空格去掉
        words[3] = words[3].replaceAll(" ", "");
        //用一个buff重新拼接ETL结束
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < words.length; i++) {

            if (i < 9) {
                //拼接前面9个字段内容
                sb.append(words[i] + '\t');
            } else {
                //拼接第10个字段内容，将里面的分隔符转换为&与第4个字段的统一！
                sb.append(words[i] + '&');
            }

        }


        String result = sb.toString();

        //去除最后一次拼接时留下的&

        return result.substring(0, result.length() - 1);


    }

    @Test
    public static void main(String[] args) {

        String string = "yAr26YhuYNY\tjaybobed\t596\tPeople & Blogs\t186\t10181\t3.49\t494\t257\trjnbgpPJUksudr9sLkoZ0s\t3IU1GyX_zio\t0E7Egr8Y1YI";
        //String string = "People & Blogs\t186\t10181\t3.49\t494\t257\trjnbgpPJUksudr9sLkoZ0s\t3IU1GyX_zio\t0E7Egr8Y1YI";

        System.out.println(parse(string));

    }


}
