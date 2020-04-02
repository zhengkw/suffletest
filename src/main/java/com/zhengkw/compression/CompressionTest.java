package com.zhengkw.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import static java.lang.System.out;


/**
 * @ClassName:CompressionTest
 * @author: zhengkw
 * @description: /**
 * <p>
 * *
 * * CompressionCodec: 压缩格式的基类，可以产生一对使用此压缩格式压缩和解压缩的流！
 * *      不管是压缩和解压缩，都需要先创建指定的CompressionCodec对象！
 * *
 * *          压缩：  使用普通的流读取文件！
 * *                  再使用带压缩的输出流讲数据写出即可！
 * *
 * *          解压缩：  使用带压缩的输入流读取文件！
 * *                    再使用不带压缩格式的输出流讲数据写出！
 * *
 * @date:20/02/28下午 9:25
 * @version:1.0
 * @since:jdk1.8
 */

public class CompressionTest {

    /**
     * @descrption:
     * @return: void
     * @date: 20/02/28 下午 10:37
     * @author: zhengkw
     */
    public static void MyCompreesion() throws Exception {

        Path input = new Path("F:\\mrinput\\wordcount\\悲惨世界(英文版).txt");


        String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
        //来自于CompressionCodecFactory的构造器
        Class<?> codecClass = Class.forName(codecClassName);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //  实例化一个codec
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        //输出路径
        /**
         * Get the default filename extension for this kind of compression.
         * @return the extension including the '.'
         */
        Path output = new Path("f:/output7/悲惨世界(英文版)" + codec.getDefaultExtension());
        //获取可以使用压缩的输出流（压缩流）
        /**
         * Create an FSDataOutputStream at the indicated Path.
         * @param f the file to create
         * @param overwrite if a file with this name already exists, then if true,
         *   the file will be overwritten, and if false an exception will be thrown.
         */
        CompressionOutputStream fos = codec.createOutputStream(fs.create(output, true));
        //获取可以读取文件的输入流(普通流)
        FSDataInputStream fis = fs.open(input);

        //使用工具完成数据的拷贝
        IOUtils.copyBytes(fis, fos, conf, true);
        out.println("----------------------------over");
    }

    public static void unCompression() throws Exception {

        //输入路径
        Path input = new Path("f:/output7/悲惨世界(英文版).gz");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        //根据文件的后缀名获取压缩的编解码器
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(input);

        //输出路径
        Path output = new Path("f:/output7/悲惨世界(英文版)123.txt");
        //获取可以使用解压缩的输入流
        CompressionInputStream is = codec.createInputStream(fs.open(input));

        //获取可以读取文件的输入流
        FSDataOutputStream os = fs.create(output, true);

        //使用工具完成数据的拷贝
        IOUtils.copyBytes(is, os, conf, true);
        out.println("----------------------------over1");
    }

    public static void main(String[] args) throws Exception {

       // CompressionTest.MyCompreesion();


        CompressionTest.unCompression();
    }
}
