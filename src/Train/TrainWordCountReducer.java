package Train;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TrainWordCountReducer extends Reducer<Text, MapWritable, Text, IntWritable>{
   
    private MultipleOutputs<Text, IntWritable> multipleOutputs = null;
    private IntWritable wordCount = new IntWritable();
    private String outputFilePrefix ;
    public void setPrefix (String thePrefix)
    {   //
    	outputFilePrefix = thePrefix;
    }
    public String getPrefix()
    {
    	return outputFilePrefix;
    }
	protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		
		Map<Text,Integer> result = new HashMap<Text,Integer>();
		for (MapWritable value: values){
			for(Writable word: value.keySet())
			{
				Text textWord = (Text)word;
				int intValue = ((IntWritable)value.get(textWord)).get();
				if(result.containsKey(textWord))
				{    
					intValue = intValue + result.get(textWord);
					result.put(textWord,intValue);
				}
				else
				{
					result.put(textWord, intValue);
				}
			}
		}
		for(Text word: result.keySet())
		{
			//System.out.println(word.toString()+"," + result.get(word).toString());
			wordCount.set(result.get(word));
			System.out.printf("prefix %s\n", outputFilePrefix + "_" + key.toString());
			multipleOutputs.write(new Text(word), wordCount, outputFilePrefix + "_" + key.toString());
			//multipleOutputs.write(new Text(word), new DoubleWritable(result.get(word)/wordSum), "Class_" + key.toString());
		}
		//context.write(key, new IntWritable(count));
	}
	protected void setup(Context context) throws IOException,InterruptedException {
		multipleOutputs =  new MultipleOutputs<Text,IntWritable>(context);
		Configuration conf = new Configuration();
		conf.addResource("NaiveBayesConfig.xml");
		outputFilePrefix = conf.get("TrainWordCountOutputPrefix"); 
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		multipleOutputs.close();
	}
}
