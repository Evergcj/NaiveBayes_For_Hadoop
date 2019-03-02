package TestFileCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Train.FileWordRecordReader;
import Train.TrainWordCountCombiner;
import Train.TrainWordCountMapper;
import Train.TrainWordCountReducer;

class TestFileRecordReader extends FileWordRecordReader {
	private FileSplit fileSplit;
	private Text key = new Text();
	private Configuration conf ;
	public void initialize(InputSplit split, TaskAttemptContext context) 
	                                       throws IOException,InterruptedException {
		fileSplit = (FileSplit)split;
		conf = context.getConfiguration();
		conf.addResource("NaiveBayesConfig.xml");
		super.initialize(split, context);
	}
	public Text getCurrentKey() {
		String pathName = fileSplit.getPath().toString();
		String fs = conf.get("FileSystem");
		pathName = pathName.replaceAll(fs + "/", ""); // ��urlȥ��
		pathName = pathName.replace('/', '-'); // ���ļ����Ÿĳ�-
	    key.set(pathName);
		return key;
	}
}
class TestFileInputFormat extends FileInputFormat<Text, MapWritable>  {
//	protected boolean isSplitable(JobContext context,Path file) {
//		return false;
//	}
	public RecordReader<Text,MapWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException{
		TestFileRecordReader reader = new TestFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}
}
// Mapper ����TrainWordCountMapper
// Combiner ����TrainWordCountCombiner
// 
// Reducer �̳�TrainWordCountReducer
class TestFileWordCountReducer extends TrainWordCountReducer {
	  protected void setup(Context context) throws IOException,InterruptedException {
		  super.setup(context);
		  Configuration conf = new Configuration();
		  conf.addResource("NaiveBayesConfig.xml");
		  super.setPrefix(conf.get("TestWordCountOutputPrefix"));//����ǰ׺
	  }
}
public class TestFileWordCount {
//	static {
//	    try {
//	    	System.load("D:/software/hadoop-2.7.3/bin/hadoop.dll");
//	    } catch (UnsatisfiedLinkError e) {
//	      System.err.println("Native code library failed to load.\n" + e);
//	      System.exit(1);
//	    }
//	}
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.addResource("NaiveBayesConfig.xml");
		Job job = Job.getInstance(conf,"fileWordCount");
		job.setJarByClass(TestFileWordCount.class);
		job.setInputFormatClass(TestFileInputFormat.class);

		job.setMapperClass(TrainWordCountMapper.class);
		job.setCombinerClass(TrainWordCountCombiner.class);
		job.setReducerClass(TestFileWordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));
		String fs = conf.get("FileSystem");
		// ��ȡ�����ļ�
		String output = conf.get("TestWordCountOutput"); 
		// ƴ������ļ���ַ
		output = fs + "/" + output;
		FileOutputFormat.setOutputPath(job, new Path(output));
		if(!job.waitForCompletion(true)) {
			System.out.println("file word count task fail!");
		}
		
	}
}