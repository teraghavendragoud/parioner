package partitionerexample;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class PartitionerExample extends Configured implements Tool
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {

	   String mapInfo ;
      public void map(LongWritable key, Text value, Context context)
      {
         try{
            String[] str = value.toString().split("\\,");
            String gender=str[3];
            mapInfo = str + "  MAP INFO :" + "text is " + gender + "Value is " + value.toString();
            context.write(new Text(gender), new Text(mapInfo));
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
   
   //Reducer class
	
 public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
   {
      public int max = -1;
      static int counter = 0;
      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
         max = -1;
         counter = counter + 1;
         
         for (Text val : values)
         {
        	 context.write(new Text(val), new IntWritable(counter));	
            String [] str = val.toString().split("\\,");
            int iSal = Integer.parseInt(str[4]);
            if(iSal >max)
            	max= iSal;
            	
         }
			
         context.write(new Text(key), new IntWritable(max));
      }
   }
   
   //Partitioner class
	
   public static class CaderPartitioner extends
   Partitioner < Text, Text >
   {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
         String[] str = value.toString().split("\\,");
         int age = Integer.parseInt(str[2]);
         
         if(numReduceTasks == 0)
         {
            return 0;
         }
         
         if(age<=20)
         {
            return 0;
         }
         else if(age>20 && age<=30)
         {
            return 1 % numReduceTasks;
         }
         else
         {
            return 2 % numReduceTasks;
         }
      }
   }
   
   public static class CaderPartitioner2 extends
   Partitioner < Text, Text >
   {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
         String[] str = value.toString().split("\\,");
         String strGender = str[3];
         
         if(numReduceTasks == 0)
         {
            return 0;
         }
         
         if(strGender.contains("Male"))
         {
            return 0;
         }         
         else
         {
            return 1 % numReduceTasks;
         }
      }
   }
   
   @Override
   public int run(String[] arg) throws Exception
   {
      Configuration conf = getConf();
		
      Job job = new Job(conf, "TERG");
      job.setJarByClass(PartitionerExample.class);
      System.out.println("***************************** "+ arg[0]);
      System.out.println("***************************** "+ arg[1]);
      FileInputFormat.setInputPaths(job, new Path(arg[0]));
      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
		
      job.setMapperClass(MapClass.class);
      job.setInputFormatClass(TextInputFormat.class);
		
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
		
      //job.setPartitionerClass(CaderPartitioner.class);
     job.setPartitionerClass(CaderPartitioner2.class);
      job.setReducerClass(ReduceClass.class);
      job.setNumReduceTasks(2);
      job.setInputFormatClass(TextInputFormat.class);
		
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
      return 0;
   }
   
  
   
   public static void main(String ar[]) throws Exception
   {
	   System.out.println("**************************************************************************");
     int res = ToolRunner.run(new Configuration(), new PartitionerExample(),ar);
	   
	 
      System.exit(0);
   }
   
   
   
}