package Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NBTestReducer extends Reducer<Text, MapWritable, NullWritable, Text>{
	private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
	private static Map<String,Double>  classProbability  = new HashMap<String,Double>();
	private static boolean  hasLoadData = false;
	protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		Map<String,Double> probability = new HashMap<String,Double>();
		String strClassName;
		
		for (MapWritable value: values){
			for(Writable className:value.keySet())
			{   // 累加每个单词在该分类下的特征权重,即log(/)
				strClassName = ((Text)className).toString();
				double scores = ((DoubleWritable)value.get(className)).get();
				if(probability.containsKey(strClassName))
				{
					probability.put(strClassName,probability.get(strClassName) + scores);
				}
				else
				{   
					if(classProbability.containsKey(strClassName))
						probability.put(strClassName, scores + classProbability.get(strClassName));
					else
						probability.put(strClassName, scores);
				}
			}
		}
		String wordClass = "Fault";
		double maxScores = -Double.MAX_VALUE;
		for(String className: probability.keySet())
		{
			if(probability.get(className) > maxScores)
			{
				wordClass = className;
				maxScores = probability.get(className);
			}
		}
		multipleOutputs.write(NullWritable.get(), key,wordClass);
	}
	protected void setup(Context context) throws IOException,InterruptedException {
		multipleOutputs =  new MultipleOutputs<NullWritable,Text>(context);
		if(!hasLoadData)
		{
			ReadClassCount();
			hasLoadData = true;
		}
	}
	// 读入训练集每个分类的文件个数，一个实例只允许读入一次
	private void ReadClassCount() throws IOException,InterruptedException
	{
		Configuration configuration = new Configuration();
		configuration.addResource("NaiveBayesConfig.xml");
		String fs = configuration.get("FileSystem");
		String output = configuration.get("FileCountOutput");
		output = fs + "/" + output;// 获得文件目录
	    FileSystem fileSystem = FileSystem.get(URI.create(output), configuration);
	    Path filePath = new Path(output);// 获取目录下的所有文件，不递归
	    FileStatus stats[] = fileSystem.listStatus(filePath); 
	    double classCount = 0;
        for(int i = 0; i < stats.length; ++i){
            //文件读取part_-r-00000这种格式的
            if(stats[i].getPath().getName().indexOf("part") != -1){
            	Path inFile = new Path(stats[i].getPath().toString());  
                FSDataInputStream in = null;  
                in = fileSystem.open(inFile);  
                InputStreamReader isr = new InputStreamReader(in,"utf-8");
                BufferedReader br = new BufferedReader(isr);
                String line; // 按行读取
                while((line = br.readLine()) != null){
                    String[] values = line.split("\t");
                    if (values.length == 2)
                    {
                    	double intValue = Double.parseDouble(values[1]);
                    	classProbability.put(values[0].trim(), intValue);
                    	classCount += intValue; // 统计训练集文件个数
                    }
                }
            }
        }
        for(String className: classProbability.keySet())
        {    // 计算每个分类文件个数占总文件的百分比，并求对数，表示先验概率。
        	double prob = Math.log(classProbability.get(className)/classCount);
        	classProbability.put(className, prob);
        }
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}