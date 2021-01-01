package com.colbert.hadoop.prdict;

import com.colbert.hadoop.utils.Filter;
import com.colbert.hadoop.utils.VariableUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * @Description 预测的mapper类
 * @Date 2021/1/1 18:36
 * @Author Colbert
 */
public class PrMapper extends Mapper<LongWritable, Text, IntWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] content = value.toString().split("\t");
        // 行号依次递增
        IntWritable count = new IntWritable(VariableUtils.lineCount);
        if (VariableUtils.lineCount <= 2000) {
            if (content.length == VariableUtils.VALUE_ABLE_LENGTH) {
                String words = content[1];
                String[] word = words.split(" ");
                for (String s : word) {
                    // 过滤非中文
                    if (Filter.chineseFilter(s)) {
                        context.write(count, new Text(s));
                    }
                }
            }
        }
        VariableUtils.lineCount++;
    }
}
