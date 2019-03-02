package Test;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class NBTestCombiner extends Reducer<Text, MapWritable, Text, MapWritable>{

	protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		MapWritable probability = new MapWritable();
		Text txtClassName = new Text();
		for (MapWritable value: values){
			for(Writable className:value.keySet())
			{
				txtClassName = (Text)className;
				DoubleWritable scores = (DoubleWritable)value.get(className);
				if(probability.containsKey(txtClassName))
				{
					scores.set(scores.get() + ((DoubleWritable)probability.get(txtClassName)).get());
					probability.put(txtClassName,scores);
				}
				else
				{
					probability.put(txtClassName, scores);
				}
			}
		}
		context.write(key, probability);
	}
}