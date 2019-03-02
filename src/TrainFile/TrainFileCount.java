package TrainFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class TrainFileCountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
	private static int nums = 0;
    //private NBParser parser = new NBParser();
    @Override
	protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
		context.write(key, value);
	}
//    protected void setup(Context context) throws IOException,InterruptedException {
//    	nums ++;
//		System.out.printf("==========%d=====",nums);
//	}
}
class TrainFileCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int sum = 0;
		for (IntWritable value: values){
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
	private static int nums = 0;
//	protected void setup(Context context) throws IOException,InterruptedException {
//    	nums ++;
////		System.out.printf("==========%d=====",nums);
//	}
}
public class TrainFileCount {
//	static {
//	    try {
//	    	System.load("D:/software/hadoop-2.7.3/bin/hadoop.dll");
//	    } catch (UnsatisfiedLinkError e) {
//	      System.err.println("Native code library failed to load.\n" + e);
//	      System.exit(1);
//	    }
//	   }
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.addResource("NaiveBayesConfig.xml");
		Job job = Job.getInstance(conf,"filecount");
		job.setJarByClass(TrainFileCount.class);

		job.setMapperClass(TrainFileCountMapper.class);
		job.setCombinerClass(TrainFileCountReducer.class);
		job.setReducerClass(TrainFileCountReducer.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));
		String fs = conf.get("FileSystem");
		String output = conf.get("FileCountOutput");
		output = fs + "/" + output;
		FileOutputFormat.setOutputPath(job, new Path(output));
		if(!job.waitForCompletion(true)) {
			System.out.println("=========file count task fail!==========");
		}
		
	}
}
