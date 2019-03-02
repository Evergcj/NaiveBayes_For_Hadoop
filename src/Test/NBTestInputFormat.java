package Test;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class NBTestInputFormat extends FileInputFormat<Text, Text>  {
	protected boolean isSplitable(JobContext context,Path file) {
		return false;
	}
	public RecordReader<Text,Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException{
		NBTestRecordReader reader = new NBTestRecordReader();
		reader.initialize(split, context);
		return reader;
	}
}

