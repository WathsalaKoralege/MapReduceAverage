package com.wathsala.emr;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Average {

   public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
      private Text employeeID = new Text();
      private Text loginTime = new Text();

      public void map(LongWritable key, Text empRecord, Context context) throws IOException, InterruptedException {
         String line = empRecord.toString();
         StringTokenizer tokenizer = new StringTokenizer(line," ");
         while (tokenizer.hasMoreTokens()) {
            employeeID.set(tokenizer.nextToken());
            loginTime.set(tokenizer.nextToken());
            Float timeFloat = Float.parseFloat(String.valueOf(loginTime));
            context.write(employeeID, new FloatWritable(timeFloat));
         }
      }
   }

   public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

      public void reduce(Text key, Iterable<FloatWritable> values, Context context)
         throws IOException, InterruptedException {
         int count = 0;
         Float totalTime = (float) 0;
         for (FloatWritable val : values) {
            totalTime += val.get();
            count += 1;
         }
         Float averageLoginTime = totalTime / count;
         context.write(key, new FloatWritable(averageLoginTime));
      }
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();

      Job job = Job.getInstance(conf, "AverageLoginTime");

      job.setJarByClass(Average.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
   }

}