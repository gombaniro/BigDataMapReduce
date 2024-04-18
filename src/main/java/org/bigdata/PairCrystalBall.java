package org.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
// hdfs dfs -rmdir /user/crystalball/output
// hadoop jar Pair-Crystal-Ball-mapred-1.0-SNAPSHOT.jar org.bigdata.PairCrystalBall /user/crystalball/input /user/crystalball/output
public class PairCrystalBall {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Pair Crystal Ball");
        job.setJarByClass(PairCrystalBall.class);
        job.setMapperClass(PairCrystalBallMapper.class);
        job.setReducerClass(PairCrystalBallReducer.class);
        job.setPartitionerClass(PairPartitioner.class);
        job.setMapOutputKeyClass(PairTuple.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class PairCrystalBallMapper extends Mapper<Object, Text, PairTuple, LongWritable> {
        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, PairTuple, LongWritable>.Context context) throws IOException, InterruptedException {
            List<String> products = Arrays.stream(value.toString()
                            .split(" "))
                    .map(s -> s.trim())
                    .filter(s -> !s.isBlank())
                    .collect(Collectors.toList());

            List<ProductPair> pairs = WindowMaker.make(products);
            for (ProductPair pair : pairs) {
                for (String product : pair.getProducts()) {
                    context.write(new PairTuple(pair.getKey(), product), new LongWritable(1));
                    context.write(new PairTuple(pair.getKey(), "*"), new LongWritable(1));
                }
            }
        }
    }

    public static class PairCrystalBallReducer extends Reducer<PairTuple, LongWritable, Text, DoubleWritable> {
        private LongWritable total = new LongWritable(0);
        private String currentFirst = null;

        @Override
        protected void reduce(PairTuple key, Iterable<LongWritable> values,
                              Reducer<PairTuple, LongWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            if (currentFirst == null || !currentFirst.equals(key.getValue1())) {
                total.set(0);
                currentFirst = key.getValue1();
            }

            String value2 = key.getValue2();
            if (value2.equals("*")) {
                for (LongWritable val : values) {
                     total.set(total.get() + val.get());
                }
            } else {
                long count = 0;
                for (LongWritable val : values) {
                    count += val.get();
                }
                context.write(new Text(key.toString()),
                        new DoubleWritable( (double) count / (double) total.get()));
            }
        }
    }
}
