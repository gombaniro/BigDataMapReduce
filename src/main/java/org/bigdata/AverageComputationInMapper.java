package org.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.common.CountBytesTuple;
import org.bigdata.common.IPAddressValidator;
import org.bigdata.common.LogEntry;
import org.bigdata.common.LogParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AverageComputationInMapper {
    public static class AverageMapper extends Mapper<Object, Text, Text, CountBytesTuple> {

        Map<String, CountBytesTuple> results;

        @Override
        protected void setup(Mapper<Object, Text, Text, CountBytesTuple>.Context context) throws IOException, InterruptedException {
            results = new HashMap<>();
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, CountBytesTuple>.Context context) throws IOException, InterruptedException {
            // Common Log Format (Apache)
            LogEntry logEntry = LogParser.parseLogEntry(value.toString());
            if (!Objects.isNull(logEntry) && IPAddressValidator.isValidIPv4(logEntry.getIpAddress())) {
                 if (results.containsKey(logEntry.getIpAddress())) {
                     CountBytesTuple tuple = results.get(logEntry.getIpAddress());
                     tuple.setSentBytes(tuple.getSentBytes() + logEntry.getBytesSent());
                     tuple.setCount(tuple.getCount() + 1);
                 } else {
                     CountBytesTuple tuple = new CountBytesTuple();
                     tuple.setCount(1);
                     tuple.setSentBytes(logEntry.getBytesSent());
                     results.put(logEntry.getIpAddress(), tuple);
                 }

            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, CountBytesTuple>.Context context) throws IOException, InterruptedException {
            for(Map.Entry<String, CountBytesTuple> result: results.entrySet()) {
                context.write(new Text(result.getKey()), result.getValue());
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, CountBytesTuple, Text,  LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<CountBytesTuple> values, Reducer<Text, CountBytesTuple, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;
            for(CountBytesTuple tuple: values) {
                sum += tuple.getSentBytes();
                count += tuple.getCount();
            }
            context.write(key, new LongWritable(sum/ count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average computation in-mapper");
        job.setJarByClass(AverageComputationInMapper.class);
        job.setMapperClass(AverageComputationInMapper.AverageMapper.class);
        job.setReducerClass(AverageComputationInMapper.AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
