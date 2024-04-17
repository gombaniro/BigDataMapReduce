package org.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.common.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PairCrystalBall {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Pair Crystal Ball");
        job.setJarByClass(PairCrystalBall.class);
        job.setMapperClass(PairCrystalBallMapper.class);
        job.setReducerClass(PairCrystalBallReducer.class);
        job.setPartitionerClass(PairPartitioner.class);
        job.setSortComparatorClass(PairComparator.class);
        job.setOutputKeyClass(PairTuple.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("!!!!!Completed!!!");
    }

    public static class PairCrystalBallMapper extends Mapper<Object, Text, PairTuple, IntWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, PairTuple, IntWritable>.Context context) throws IOException, InterruptedException {
            List<String> products = Arrays.stream(value.toString()
                            .split(" "))
                    .map(s -> s.trim())
                    .filter(s -> !s.isBlank())
                    .collect(Collectors.toList());

            List<ProductPair> pairs = WindowMaker.make(products);
            for (ProductPair pair : pairs) {
                for (String product : pair.getProducts()) {
                    context.write(new PairTuple(pair.getKey(), product), new IntWritable(1));
                    context.write(new PairTuple(pair.getKey(), "*"), new IntWritable(1));
                }
            }
        }
    }

    public static class PairCrystalBallReducer extends Reducer<PairTuple, IntWritable, PairTuple, DoubleWritable> {

        private LongWritable total = new LongWritable(0);

        @Override
        protected void reduce(PairTuple key, Iterable<IntWritable> values, Reducer<PairTuple, IntWritable, PairTuple, DoubleWritable>.Context context) throws IOException, InterruptedException {

            String value2 = key.getValue2();
            if (value2.equals("*")) {
                for (IntWritable val : values) {
                     total.set(total.get() + val.get());
                }
            } else {
                long count = 0;
                for (IntWritable val : values) {
                    count += val.get();
                }
                context.write(key, new DoubleWritable((double) count / total.get()));
            }
        }
    }
}
