package org.example.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.HashSet;

public class IndexerJob {

    
    public int run(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IndexerJob");
        job.setJarByClass(IndexerJob.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (job.waitForCompletion(true)) {
            System.out.println("Job completed successfully");
            return 0;
        }
        return 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text documentId;
        private final Text word = new Text();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String filename =
                    ((FileSplit) context.getInputSplit()).getPath().getName();
            documentId = new Text(filename);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (String token : StringUtils.split(value.toString())) {
                word.set(token);
                context.write(word, documentId);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final Text docIds = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            HashSet<String> uniqueDocIds = new HashSet<>();
            for (Text docId : values) {
                uniqueDocIds.add(docId.toString());
            }
            docIds.set(new Text(StringUtils.join(",", uniqueDocIds)));
            context.write(key, docIds);
        }


    }
}
