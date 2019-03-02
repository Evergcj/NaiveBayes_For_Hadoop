package Test;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;


public class NBTestRecordReader extends RecordReader<Text, Text> {
	private FileSplit fileSplit;
	private Text key = new Text();
	LineReader lineReader = null;
	private LineRecordReader lineRecordReader = new LineRecordReader();

	public void initialize(InputSplit split, TaskAttemptContext context) 
	                                       throws IOException,InterruptedException {
		fileSplit = (FileSplit)split;
		lineRecordReader.initialize(split, context);
	}
	public Text getCurrentKey() {
		//return ;
		String pathName = fileSplit.getPath().getName();
		key.set(pathName);
		return key;
	}
	public Text getCurrentValue() {
		return lineRecordReader.getCurrentValue();
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