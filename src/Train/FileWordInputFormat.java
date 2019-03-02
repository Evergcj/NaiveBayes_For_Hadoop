package Train;

import java.io.IOException;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class FileWordInputFormat extends FileInputFormat<Text,MapWritable>  {
	public RecordReader<Text,MapWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException{
		FileWordRecordReader reader = new FileWordRecordReader();
		reader.initialize(split, context);
		return reader;
	}
}