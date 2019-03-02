package TrainFile;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<Text,IntWritable> {
	private FileSplit fileSplit;
	private Text key = new Text();
	private boolean processed = false;
	private IntWritable one = new IntWritable(1);
	private Configuration conf ;
	public void initialize(InputSplit split, TaskAttemptContext context) 
	                                       throws IOException,InterruptedException {
		this.fileSplit = (FileSplit)split;
		conf = context.getConfiguration();
	}
	public Text getCurrentKey() {
		return key;
	}
	public IntWritable getCurrentValue() {
		return one;
	}
	@Override
	public boolean nextKeyValue() throws IOException,InterruptedException {
		if(!processed) {
			Path path = fileSplit.getPath();
			String pathName = path.getParent().getName();
			conf.addResource("NaiveBayesConfig.xml");
			String prefix = conf.get("FileCountOutputPrefix");
		    key.set(prefix +"_" + pathName);
			processed = true;
			return true;
		}
		return false;
	}
	@Override
	public void close() throws IOException {}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f:0.0f;
	}
}