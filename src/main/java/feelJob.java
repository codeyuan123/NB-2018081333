import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class feelJob {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		FileSystem fileSystem = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "feelmodel");
		job.setJarByClass(feelJob.class);
		job.setMapperClass(feelMapper.class);
		job.setCombinerClass(feelReducer.class);
		job.setReducerClass(feelReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat
				.setInputPaths(job, new Path("hdfs://master:9000/input_2017082040/training.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://master:9000/output/2017082040_模型"));
		
		int isSuccessed = job.waitForCompletion(true) ? 0 : 1;
		if (isSuccessed == 0) {
			System.out.println("执行成功");
	        fileSystem.copyToLocalFile(new Path(
					"hdfs://master:9000/output/2017082040_模型/part-r-00000"), new Path("/root/Documents/2017082040_模型.txt"));
	        System.exit(isSuccessed);
		}
		
	}
}
