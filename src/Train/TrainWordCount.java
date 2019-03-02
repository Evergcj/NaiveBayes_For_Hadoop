package Train;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TrainWordCount {
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
		
		Job job = Job.getInstance(conf,"wordcount");
		job.setJarByClass(TrainWordCount.class);
		job.setInputFormatClass(FileWordInputFormat.class);

		job.setMapperClass(TrainWordCountMapper.class);
		job.setCombinerClass(TrainWordCountCombiner.class);
		job.setReducerClass(TrainWordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));
		String fs = conf.get("FileSystem");
		String output = conf.get("TrainWordCountOutput");
		output = fs + "/" + output;
		FileOutputFormat.setOutputPath(job, new Path(output));
		if(!job.waitForCompletion(true)) {
			System.out.println("word count task fail!");
		}
		
	}
}
