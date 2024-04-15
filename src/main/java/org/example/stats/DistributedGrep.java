package org.example.stats;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistributedGrep {
    public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {
        private String mapRegex = null;

        @Override
        protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            mapRegex = context.getConfiguration().get("mapregex");
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            if(value.toString().matches(mapRegex)){
                context.write(NullWritable.get(), value);
            }
        }
    }
}
