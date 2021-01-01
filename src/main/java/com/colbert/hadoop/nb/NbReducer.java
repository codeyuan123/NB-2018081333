package com.colbert.hadoop.nb;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @Description mapper对计数好评价信息进行计数
 * @Date 2021/1/1 14:17
 * @Author Colbert
 */
public class NbReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // 对相同key的进行累加计数，最后得出总的条数
        for (IntWritable value : values) {
            count++;
        }
        context.write(key, new IntWritable(count));
    }
}
