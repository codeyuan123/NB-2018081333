package com.colbert.hadoop.nb;


import com.colbert.hadoop.prdict.PrMapper;
import com.colbert.hadoop.prdict.PrReducer;
import com.colbert.hadoop.utils.VariableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * @Description 程序入口
 * @Date 2021/1/1 13:33
 * @Author Colbert
 */
public class NB {
    public static Map<String, Integer> map = new HashMap<>();

    public static void main(String[] args) {
        final String URI = "hdfs://192.168.158.140:9000";
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", URI);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            boolean exists = fs.exists(new Path("/output_2018081333"));
            Job job = Job.getInstance(conf);
            job.setJarByClass(NB.class);
            // 根据模型是否存在确定任务是训练还是预测
            if (!exists) {
                job.setMapperClass(NbMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);
                job.setReducerClass(NbReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                FileInputFormat.setInputPaths(job, new Path("/input_2018081333/training.txt"));
                FileOutputFormat.setOutputPath(job, new Path("/output_2018081333"));
                boolean bool = job.waitForCompletion(true);
                System.out.println(bool ? "训练成功" : "训练失败");
            } else {
                // 进行预测
                FSDataInputStream in = fs.open(new Path("/output_2018081333/part-r-00000"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                System.out.println(reader);
                String line = "";
                // 一行一行地读入
                while ((line = reader.readLine()) != null) {
                    // 根据空格切分
                    String[] split = line.split("\\s");
                    if (split.length == VariableUtils.VALUE_ABLE_LENGTH) {
                        // 将得到的数据存入静态map中，等待下一步与测试数据比对
                        map.put(split[0], Integer.parseInt(split[1]));
                    }
                }
                reader.close();
                in.close();
                job.setMapperClass(PrMapper.class);
                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(PrReducer.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.setInputPaths(job, new Path("/input_2018081333/test.txt"));
                FileOutputFormat.setOutputPath(job, new Path("/output_2020"));
                boolean bool = job.waitForCompletion(true);
                System.out.println(bool ? "预测成功" : "预测失败");
                fs.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
