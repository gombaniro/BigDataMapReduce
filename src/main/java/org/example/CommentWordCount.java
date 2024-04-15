package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class CommentWordCount {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            // Parse the input String into a nice map
            Map<String, String> parsed = XMLParserUtil.transformXmlToMap(value.toString());
            // Grab the "Text" field , since that is what we are counting over
            String txt = parsed.get("Text");
            // .get will return null if the ky is not there
            if(txt == null) {
                // skip this record
                return;
            }
            // Remove some annoying punctuation
            txt = txt.replaceAll("'", ""); // remove single quotes (e.g. can't)
            txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a space

            // Tokenize the string by splitting it up on whitespace into
            // something we can iterate over,
            // then send the tokens away
            StringTokenizer itr = new StringTokenizer(txt);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    public static class InSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
              private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
           int sum = 0;
           for(IntWritable val : values) {
               sum += val.get();
           }
           result.set(sum);
           context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length != 2) {
            System.err.println("Usage : CommentWordCount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"StackOverflow comment word count");
        job.setJarByClass(CommentWordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(InSumReducer.class);
        job.setReducerClass(InSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)? 0: 1);
    }

}
