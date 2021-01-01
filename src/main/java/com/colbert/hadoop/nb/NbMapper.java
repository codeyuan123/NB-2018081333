package com.colbert.hadoop.nb;

import com.colbert.hadoop.utils.VariableUtils;
import com.colbert.hadoop.utils.Filter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description 对评价和词性进行分割计数
 * @Date 2021/1/1 13:41
 * @Author Colbert
 */
public class NbMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    /**
     * 分割后可以进行下一步分割的字符串数组长度
     */


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 词性与评价分开
        String[] content = value.toString().split("\t");

        //如果只有好差评，没有其他评价的要过滤
        if (content.length == VariableUtils.VALUE_ABLE_LENGTH) {
            String evaluates = content[1];
                // 将具体的评价按照空格分割
                String[] evaluate = evaluates.split(" ");
                for (String s : evaluate) {
                    // 过滤非中文
                    if (Filter.chineseFilter(s)) {
                        context.write(new Text(content[0] + "_" + s), new IntWritable(1));
                    }
                }
                // 将所有的好评与差评进行统计
                context.write(new Text("统计_"+content[0]), new IntWritable(1));
            }
    }


}
