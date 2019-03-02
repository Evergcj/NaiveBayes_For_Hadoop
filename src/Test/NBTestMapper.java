package Test;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NBTestMapper extends Mapper<Text,Text,Text,MapWritable>{
	
	private static Map<String,Map<Integer,Double>> classifiedWordMap = new HashMap<String,Map<Integer, Double>>();
	private static Map<String, Integer> vocabulary = new HashMap<String,Integer>();
	private Text txtClassName;
	private DoubleWritable score;
	private static boolean hasLoad = false;
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		String word = new String();
		int wordCount = 0;
		if(tokenizer.hasMoreTokens())
		{
			word = tokenizer.nextToken();
		}
		if(tokenizer.hasMoreTokens())
		{
			wordCount = Integer.parseInt(tokenizer.nextToken());
		}
		MapWritable result = new MapWritable();
		int wordID = -2;// count 占据了一个-1;
		if(vocabulary.containsKey(word))
			wordID = vocabulary.get(word);
		for(String className: classifiedWordMap.keySet())
		{
			double part = 0;
			if(classifiedWordMap.get(className).containsKey(wordID))
				part = classifiedWordMap.get(className).get(wordID);
			else {
				part = 1 / (classifiedWordMap.get(className).get(-1)+ vocabulary.size());
			}
			txtClassName = new Text(className);
			score = new DoubleWritable(part * wordCount);
			result.put(txtClassName, score);
		}
		context.write(key, result);
	}
	private void ReadClassCount() throws IOException,InterruptedException
	{
		Configuration configuration = new Configuration();
		configuration.addResource("NaiveBayesConfig.xml");
		String prefix = configuration.get("TrainWordCountOutputPrefix");
		String fs = configuration.get("FileSystem");
		String output = configuration.get("TrainWordCountOutput");
		output = fs +  "/" + output;
	    FileSystem fileSystem = FileSystem.get(URI.create(output), configuration);
	    Path filePath = new Path(output);
	    FileStatus stats[] = fileSystem.listStatus(filePath); 
        for(int i = 0; i < stats.length; ++i){
        	String fileName = stats[i].getPath().getName();
        	int clsStartPtr = fileName.indexOf(prefix);
        	//文件读取Class_-r-00000这种格式的
            if(clsStartPtr != -1){
            	int clsEndPtr = fileName.indexOf("r",-1);
            	String className = fileName.substring(clsStartPtr, clsEndPtr - 1);
            	Map<Integer, Double> map1 ;
            	if(classifiedWordMap.containsKey(className))
            	{
            		map1 = classifiedWordMap.get(className);
            	}
            	else
            	{
            		map1 = new HashMap<Integer, Double>();
            		map1.put(-1,0.0); // 表示count
            	}
            	Path inFile = new Path(stats[i].getPath().toString());  
                FSDataInputStream in = null;  
                in = fileSystem.open(inFile);  
                InputStreamReader isr = new InputStreamReader(in,"utf-8");
                BufferedReader br = new BufferedReader(isr);
                String line;
                double wordCount = 0;
                String word ;
                while((line = br.readLine()) != null){
                    String[] values = line.split("\t");
                    if (values.length != 2)
                    	continue;
                	double dValue = Double.parseDouble(values[1]);
                	word = values[0].trim();
                	wordCount += dValue;
                	if(!vocabulary.containsKey(word))
                		vocabulary.put(word, vocabulary.size());
                	int wordId = vocabulary.get(word);
                	if(map1.containsKey(wordId))
                		map1.put(wordId, dValue + map1.get(wordId));
                	else
                		map1.put(wordId, dValue);
                }
                //保存总数
                map1.put(-1, map1.get(-1) + wordCount);
                classifiedWordMap.put(className, map1);
            }
        }
        
        double wordCount = 0;
        double part = 0;
        double totalCount;
        for(String className: classifiedWordMap.keySet())
        {
        	totalCount = classifiedWordMap.get(className).get(-1)+ vocabulary.size();
        	for(String word:vocabulary.keySet())
            {
        		int wordID = vocabulary.get(word);
            	if(!classifiedWordMap.get(className).containsKey(wordID))
            		wordCount = 1.0;
            	else
            		wordCount = classifiedWordMap.get(className).get(wordID) + 1;// zero-padding
            	part =  Math.log(wordCount / totalCount);
            	classifiedWordMap.get(className).put(wordID, part);
            }
        }
	}
	protected void setup(Context context) throws IOException,InterruptedException {
		if(!hasLoad)
		{
			ReadClassCount();
			hasLoad = true;
		}
	}
}
