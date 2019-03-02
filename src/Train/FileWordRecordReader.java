package Train;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

public class FileWordRecordReader extends RecordReader< Text,MapWritable> {
	private FileSplit fileSplit;
	private MapWritable value;
	private Text key = new Text();
	private IntWritable one = new IntWritable(1); 
	LineReader lineReader = null;
	private LineRecordReader lineRecordReader = new LineRecordReader();

	public void initialize(InputSplit split, TaskAttemptContext context) 
	                                       throws IOException,InterruptedException {
		fileSplit = (FileSplit)split;
		lineRecordReader.initialize(split, context);
	}
	public Text getCurrentKey() {
		// 
		Path path = fileSplit.getPath();
		String pathName = path.getParent().getName();
		key.set(pathName);
		return key;
	}
	public MapWritable getCurrentValue() {
		Text word = lineRecordReader.getCurrentValue();
	    value = new MapWritable();
	    value.put(word,one);
		return value;
	}
	
	@Override
	public boolean nextKeyValue() throws IOException,InterruptedException {
		return lineRecordReader.nextKeyValue();
		
	}
	@Override
	public void close() throws IOException {
		lineRecordReader.close();
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}
	
}
