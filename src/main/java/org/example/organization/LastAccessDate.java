package org.example.organization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.XMLParserUtil;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class LastAccessDate {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Last access date partitioning");
        job.setPartitionerClass(LastAccessDatePartitioner.class);
        LastAccessDatePartitioner.setMinLastAccessDateYear(job, 2008);
        //Last
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class LastAccessDateMapper extends
            Mapper <Object, Text, IntWritable, Text> {
        //This object will format the creation date string into a Date object
        private final static DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

        private IntWritable outKey = new IntWritable();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = XMLParserUtil.transformXmlToMap(value.toString());

            // Grab the last access date
            String strDate = parsed.get("LastAccessDate");

            // Parsed the string into a Calendar object
            LocalDateTime localDateTime = LocalDateTime.parse(strDate, formatter);
            outKey.set(localDateTime.getYear());

            // Write out the year with the input value
            context.write(outKey, value);
        }
    }

    public static class ValueReducer extends
            Reducer <IntWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

            for(Text t: values) {
                context.write(t, NullWritable.get());
            }
        }
    }
}
