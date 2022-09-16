package map_reduce_demo;

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

public class WordCount {
    //input: (key,value), object in text///output: text , number
    public static class MapperSide extends Mapper<Object,Text,Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            StringTokenizer itr = new StringTokenizer(value.toString());
            final Text wordOut = new Text();
            IntWritable one = new IntWritable(1);
            while (itr.hasMoreTokens()){
                wordOut.set(itr.nextToken());
                context.write(wordOut, one);
            }
        }
    }

    public static class ReducerSide extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException{
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            IntWritable result = new IntWritable();
            result.set(count);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws IOException,Exception{
        if(args.length != 2){
            System.err.println("Usage WordCount <input_file> <output_dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperSide.class);
        job.setReducerClass(ReducerSide.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
