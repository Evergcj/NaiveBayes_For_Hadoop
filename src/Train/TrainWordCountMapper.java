package Train;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrainWordCountMapper extends Mapper<Text,MapWritable, Text, MapWritable> {
    
	
    @Override
	protected void map( Text key, MapWritable value, Context context) throws IOException, InterruptedException{
//    	System.out.println("===================current Key:" + value.toString());
		context.write(key, value);
		
	}
}

