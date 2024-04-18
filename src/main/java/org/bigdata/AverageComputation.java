package org.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.common.IPAddressValidator;
import org.bigdata.common.LogEntry;
import org.bigdata.common.LogParser;

import java.io.IOException;
import java.util.Objects;

public class AverageComputation {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average computation");
        job.setJarByClass(AverageComputation.class);
        job.setMapperClass(AverageComputation.AverageMapper.class);
        job.setReducerClass(AverageComputation.AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class AverageMapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            // host ident authuser date request status bytes
            // Common Log Format (Apache)
            LogEntry logEntry = LogParser.parseLogEntry(value.toString());
            if (!Objects.isNull(logEntry) && IPAddressValidator.isValidIPv4(logEntry.getIpAddress())) {
                context.write(new Text(logEntry.getIpAddress()), new LongWritable(logEntry.getBytesSent()));
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;
            for (LongWritable value : values) {
                sum += value.get();
                count++;
            }
            context.write(key, new LongWritable(sum / count));
        }
    }
}
