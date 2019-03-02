package Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NBTest {
	static {
	    try {
	    	System.load("D:/software/hadoop-2.7.3/bin/hadoop.dll");
	    } catch (UnsatisfiedLinkError e) {
	      System.err.println("Native code library failed to load.\n" + e);
	      System.exit(1);
	    }
	}
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.addResource("NaiveBayesConfig.xml");
		Job job = Job.getInstance(conf,"test");
		job.setJarByClass(NBTest.class);
		job.setInputFormatClass(NBTestInputFormat.class);

		job.setMapperClass(NBTestMapper.class);
		job.setCombinerClass(NBTestCombiner.class);
		job.setReducerClass(NBTestReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		String fs = conf.get("FileSystem");
		String input = conf.get("TestWordCountOutput");
		String prefix = conf.get("TestWordCountOutputPrefix");
		input = fs + "/" + input + "/" + prefix + "*";
		FileInputFormat.setInputPaths(job, new Path(input));
		
		String output = conf.get("TestResultOutput");
		output = fs + "/" + output;
		FileOutputFormat.setOutputPath(job, new Path(output));
		if(!job.waitForCompletion(true)) {
			System.out.println("test task fail!");
		}
		
	}
}
