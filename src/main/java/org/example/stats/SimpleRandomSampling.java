package org.example.stats;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class SimpleRandomSampling {

    public static class SRSMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Random rands = new Random();
        private Double percentage;

        @Override
        protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            // Retrieve the percentage that is passed in via the configuration
            // like this: config.set("filter_percentage", .5);
            // for .5%
            String strPercentage = context.getConfiguration()
                    .get("filter_percentage");

            percentage = Double.parseDouble(strPercentage) / 100.0;
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            if (rands.nextDouble() < percentage) {
                context.write(NullWritable.get(), value);
            }
        }
    }
}
