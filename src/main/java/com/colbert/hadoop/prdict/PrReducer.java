package com.colbert.hadoop.prdict;

import com.colbert.hadoop.nb.NB;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.SQLOutput;

/**
 * @Description 预测的reducer类，做一些预测与模型的比对分析
 * @Date 2021/1/1 19:25
 * @Author Colbert
 */
public class PrReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double goodCount = 0.0;
        double badCount = 0.0;
        double tempGoodCount;
        double tempBadCount;
        double goodRobability = 1.0;
        double badRobability = 1.0;
        for (Text value : values) {
            // 分别获取两种评价的数量
            if (NB.map.get("好评_" + value) != null) {
                tempGoodCount = NB.map.get("好评_" + value);
            } else {
                // 如果不存在改类别下的单词，则将null转为0
                tempGoodCount = 0;
            }
            if (NB.map.get("差评_" + value) != null) {
                tempBadCount = NB.map.get("差评_" + value);
            } else {
                tempBadCount = 0;
            }

            goodCount += tempGoodCount;
            badCount += tempBadCount;
            goodRobability = (tempGoodCount / (tempGoodCount + tempBadCount)) * goodRobability;
            badRobability =  (tempBadCount / (tempGoodCount + tempBadCount)) * badRobability;
        }
        System.out.println("good====="+goodRobability);
        System.out.println("bad======"+badRobability);
        boolean isGood = goodRobability * (goodCount / (goodCount + badCount)) >= badRobability * (badCount /(goodCount + badCount));
        System.out.println(goodRobability * (goodCount / (goodCount + badCount)));
        System.out.println(badRobability * (badCount /(goodCount + badCount)));
        System.out.println(isGood);
        context.write(key,new Text(isGood ? "好评" : "差评"));
    }
}
