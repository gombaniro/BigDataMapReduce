package org.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.common.PairTuple;
import org.bigdata.common.ProductPair;
import org.bigdata.common.WindowMaker;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


// hdfs dfs -rmdir /user/crystalball/output
// hadoop jar Stripe-Crystal-Ball-mapred-1.0-SNAPSHOT.jar org.bigdata.StripeCrystalBall /user/crystalball/input /user/crystalball/output
public class StripeCrystalBall {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stripe Crystal Ball");
        job.setJarByClass(StripeCrystalBall.class);
        job.setMapperClass(StripeCrystalBallMapper.class);
        job.setReducerClass(StripeCrystalBallReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class StripeCrystalBallMapper extends Mapper<Object, Text, Text, MapWritable> {


        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, MapWritable>.Context context) throws IOException, InterruptedException {
            List<String> products = Arrays.stream(value.toString()
                            .split(" "))
                    .map(s -> s.trim())
                    .filter(s -> !s.isBlank())
                    .collect(Collectors.toList());

            List<ProductPair> pairs = WindowMaker.make(products);
            for (ProductPair pair : pairs) {
                MapWritable results = new MapWritable();
                for (String product : pair.getProducts()) {
                    PairTuple tuple = new PairTuple(pair.getKey(), product);
                    if (results.containsKey(tuple)) {
                        DoubleWritable oldVal = (DoubleWritable) results.get(tuple);
                        oldVal.set(oldVal.get() + 1.0);
                        results.put(tuple, oldVal);
                    } else {
                        results.put(tuple, new DoubleWritable(1.0));
                    }

                }
                context.write(new Text(pair.getKey()), results);
            }
        }
    }

    public static class StripeCrystalBallReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values,
                              Reducer<Text, MapWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            MapWritable finalResults = new MapWritable();

            DoubleWritable total = new DoubleWritable(0.0);
            for (MapWritable mapWritable : values) {
                // element wise addition
                for (Map.Entry<Writable, Writable> entry : mapWritable.entrySet()) {
                    PairTuple k = (PairTuple) entry.getKey();
                    DoubleWritable val = (DoubleWritable) entry.getValue();

                    total.set(total.get() + val.get());
                    if (finalResults.containsKey(k)) {
                        DoubleWritable oldVal = (DoubleWritable) finalResults.get(k);
                        oldVal.set(oldVal.get() + val.get());
                        finalResults.put(k, oldVal);
                    } else {
                        finalResults.put(k, val);
                    }
                } // end of element wise addition
            }
            for (Map.Entry<Writable, Writable> entry : finalResults.entrySet()) {
                PairTuple outPutKey = (PairTuple) entry.getKey();
                DoubleWritable val = (DoubleWritable) entry.getValue();
                context.write(new Text(outPutKey.toString()),
                        new DoubleWritable(val.get() / total.get()));
            }
        }
    }
}
