package org.example.stats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.XMLParserUtil;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class MedianStdDev {
    public static class MedianStdDevMapper extends
            Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable outHour = new IntWritable();
        private IntWritable outCommentLength = new IntWritable();

        private final static DateTimeFormatter format =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");


        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

            Map<String, String> parsed = XMLParserUtil.transformXmlToMap(value.toString());
            // Grab "creationDate" field since it is what we are grouping by
            String strDate = parsed.get("creationDate");
            // Grab the comment to find the length
            String text = parsed.get("Text");

            // get the hour this comment was posted in
            LocalDateTime creationDateTime = LocalDateTime.parse(strDate, format);

            outHour.set(creationDateTime.getHour());
            outCommentLength.set(text.length());
            context.write(outHour, outCommentLength);

        }
    }
    public static class MedianStdDevReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {
        private MedianStdDevTuple result = new MedianStdDevTuple();
        private ArrayList<Double> commentLengths = new ArrayList<>();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple>.Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            double count = 0.0;
            commentLengths.clear();
            result.setStd(0);

            // Iterate through all input values for this key
            for(IntWritable val: values) {
                commentLengths.add((double) val.get());
                sum += val.get();
                ++count;
            }

            // sort commentLengths to calculate median
            Collections.sort(commentLengths);

            // if commentLengths is an even value, average middle two elements
            if (count % 2 == 0) {
                result.setMedian((commentLengths.get((int)count / 2 - 1)
                + commentLengths.get((int)count / 2)) / 2.0f);

            } else {
                // else , set median to middle value
                result.setMedian(commentLengths.get((int) count / 2));
            }

            // Calculate standard deviation
            double mean = sum / count;
            double sumOfSquares = 0.0;
            for(Double d: commentLengths) {
                sum += ( d - mean) * (d - mean);
            }
            result.setStd(Math.sqrt(sumOfSquares / (count - 1)));
            context.write(key, result);
        }
    }
}
