package org.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
//        job.setSortComparatorClass(PairComparator.class);
        job.setOutputKeyClass(PairTuple.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class PairCrystalBallMapper extends Mapper<Object, Text, PairTuple, DoubleWritable> {
        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, PairTuple, DoubleWritable>.Context context) throws IOException, InterruptedException {
            List<String> products = Arrays.stream(value.toString()
                            .split(" "))
                    .map(s -> s.trim())
                    .filter(s -> !s.isBlank())
                    .collect(Collectors.toList());

            List<ProductPair> pairs = WindowMaker.make(products);
            for (ProductPair pair : pairs) {
                for (String product : pair.getProducts()) {
                    context.write(new PairTuple(pair.getKey(), product), new DoubleWritable(1.0));
                    context.write(new PairTuple(pair.getKey(), "*"), new DoubleWritable(1.0));
                }
            }
        }
    }

    public static class PairCrystalBallReducer extends Reducer<PairTuple, DoubleWritable, PairTuple, DoubleWritable> {
                                                           //PairTuple, IntWritable, PairTuple, DoubleWritable
        private DoubleWritable total = new DoubleWritable(0.0);

        @Override
        protected void reduce(PairTuple key, Iterable<DoubleWritable> values,
                              Reducer<PairTuple, DoubleWritable, PairTuple, DoubleWritable>.Context context) throws IOException, InterruptedException {

            String value2 = key.getValue2();
            if (value2.equals("*")) {
                for (DoubleWritable val : values) {
                     total.set(total.get() + val.get());
                }
            } else {
                double count = 0;
                for (DoubleWritable val : values) {
                    count += val.get();
                }
//                context.write(key, new DoubleWritable( count ));
//                context.write(key, new DoubleWritable(  total.get()));
                context.write(key, new DoubleWritable( count / total.get()));
            }
        }
    }
}
