package Evaluation;

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

class F1Evaluation {
	private double gPrecision = 0,gRecall = 0,precision = 0, recall = 0;
	private int gTp = 0,gFp = 0,gFn = 0,gTn = 0;
	private FileSystem gFileSystem;
	private static int fileCount = 0;
	private static Configuration conf = new Configuration();
	
	private static Map<String,int[]> EvaluationMsg = new HashMap<String, int[]>();
	
	F1Evaluation() {
		conf.addResource("NaiveBayesConfig.xml");//添加配置文件
	}
	double F1Score(double precision,double recall) {return 2 * precision * recall / (precision + recall);}
	double F1Score(int tp,int fp, int fn, int tn) {
		precision = (double)tp / (fp + tp); 
		recall = (double)tp / (tp + fn); 
		return 2 * precision * recall / (precision + recall);
	}
	void ComuputeF1(FileStatus stats,String className) throws IOException,InterruptedException
	{
		Path inFile = new Path(stats.getPath().toString());  
		System.out.println(stats.getPath().toString());
        FSDataInputStream in =  gFileSystem.open(inFile);  
        InputStreamReader isr = new InputStreamReader(in,"utf-8");
        BufferedReader br = new BufferedReader(isr);
        String line;
        while((line = br.readLine()) != null){
            String fileName = line.trim();
            if(fileName.indexOf(className) != -1)
            	EvaluationMsg.get(className)[1] ++;
            else
            	EvaluationMsg.get(className)[2] ++;
        }
//        MicroAdd(tp,fp,fn,tn);
//        fn = EvaluationMsg.get(className)[0] - tp;
//        tn = fileCount - tp - fp - fn;
//		double curF1Score = F1Score(tp,fp,fn,tn);
//		MacroAdd();
//        return curF1Score;
	}
	
	void getTestFiles()  throws IOException, InterruptedException {
		String fs = conf.get("FileSystem");
		String fileDir = fs + "/input/Test";
        FileSystem fileSystem = FileSystem.get(URI.create(fileDir), conf);
	    Path path = new Path(fileDir);
	    FileStatus[] status = fileSystem.listStatus(path);
	    System.out.println("每个类别的文件统计");
	    fileCount = 0;
	    for(FileStatus stat: status)
	    {
	    	String className = stat.getPath().getName();
	    	FileStatus[] fstatus = fileSystem.listStatus(stat.getPath());
//	    	String prefix = conf.get("TrainWordCountOutputPrefix");
//	    	className = prefix + "_" + className;
	    	if(!EvaluationMsg.containsKey(className))
	    	{
	    		EvaluationMsg.put(className, new int[]{fstatus.length,0,0});
	    		fileCount += fstatus.length; // 测试集文件总数
	    	}
	    	System.out.printf("%s:%d\n",className,fstatus.length);
	    }
	}
	void getF1() throws IOException, InterruptedException
	{
		String fs = conf.get("FileSystem");
		String fileDir = conf.get("TestResultOutput");
		fileDir = fs + "/" + fileDir;
		gFileSystem = FileSystem.get(URI.create(fileDir), conf);
        Path filePath = new Path(fileDir);
        FileStatus stats[] = gFileSystem.listStatus(filePath);  
        System.out.println("每个分类的Precision, Recall, F1得分");
        int classNum = 0;
        String prefix = conf.get("TrainWordCountOutputPrefix");
        
        for(int i = 0; i < stats.length; ++i){  // 循环获得合法文件
        	String fileName = stats[i].getPath().getName();
        	int pos = fileName.indexOf(prefix,-1);
            if(pos != -1){
            	int clsEndPtr = fileName.indexOf("r",-1);
            	String className = fileName.substring(pos + prefix.length() + 1, clsEndPtr - 1);
            	ComuputeF1(stats[i], className);
            	classNum ++; // 计算分类个数
//            	System.out.printf("%s:%f, %f, %f",className,precision, recall, curScore);
            }
        }
        for(String clsName: EvaluationMsg.keySet())
		{
			int tp = EvaluationMsg.get(clsName)[1];
			int fp = EvaluationMsg.get(clsName)[2];
			int fn = EvaluationMsg.get(clsName)[0] - tp;
			int tn = fileCount  - fp - EvaluationMsg.get(clsName)[0];
			double curScore = F1Score(tp,fp,fn,tn);
			System.out.printf("%s:%f, %f, %f\n",clsName,precision, recall, curScore);
			MacroAdd();
			MicroAdd(tp,fp,fn,tn);
		}
        ComputeMicro(); 
        ComputeMacro(classNum);
	}
	void MicroAdd(int tp, int fp, int fn, int tn){ gTp += tp; gFp += fp; gTn += tn; gFn += fn;}
	void ComputeMicro()
	{
		double microScore = F1Score(gTp,gFp,gFn,gTn);
		System.out.printf("Micro F1 Score:%f\n", microScore);
	}
	
	void MacroAdd(){ gPrecision+= precision; gRecall+= recall;}
	void ComputeMacro(int classNums)
	{  // 求precision和recall的均值
		
		double macroScore = F1Score(gPrecision / classNums, gRecall / classNums); 
		System.out.printf("Macro F1 Score:%f\n", macroScore);
	}
}
public class NBEvaluation {

	public static void main(String[] args){
		
		F1Evaluation fe = new F1Evaluation();
		try {
			fe.getTestFiles();
			fe.getF1();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
