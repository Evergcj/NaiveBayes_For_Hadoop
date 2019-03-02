package Train;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public  class TrainWordCountCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
	protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		MapWritable result = new MapWritable();
		IntWritable times = new IntWritable();
		for (MapWritable value: values){ 
			for(Writable word: value.keySet()) 
			{
				Text textWord = (Text)word;
				times = (IntWritable)value.get(textWord);
				if(result.containsKey(textWord))
				{
					int intValue = ((IntWritable)result.get(textWord)).get();
					times.set( times.get()+ intValue);
					result.put(textWord,times);
				}
				else
					result.put(textWord, times);
			}
		}
		context.write(key, result);
	}
}