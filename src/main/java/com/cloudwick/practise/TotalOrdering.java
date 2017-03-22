package com.cloudwick.practise;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

/**
 * Created by Rajiv on 3/20/17.
 */
public class TotalOrdering {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length!=3){
            System.out.print("Not Enough arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(TotalOrdering.class);
        job.setJobName("Total Order Sorting Application");

        job.setNumReduceTasks(3);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Using default Mapper and Reducer class
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        // Define input format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);

        //Setting the output Key,Value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[2]));
        InputSampler.Sampler<Text,Text> mySampler = new InputSampler.RandomSampler<Text,Text>(0.3, 500, 1);
        InputSampler.writePartitionFile(job,mySampler);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
