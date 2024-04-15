package org.example.stats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.XMLParserUtil;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class AverageFinder {
    public static class AverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {
        private final static DateTimeFormatter format =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
        private final IntWritable outHour = new IntWritable();
        private final CountAverageTuple outCountAverage = new CountAverageTuple();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, CountAverageTuple>.Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = XMLParserUtil.transformXmlToMap(value.toString());

            // Grab "creationDate" field since it is what we are grouping by
            String strDate = parsed.get("creationDate");
            // Grab the comment to find the length
            String text = parsed.get("Text");

            // get the hour this comment was posted in
            LocalDateTime creationDateTime = LocalDateTime.parse(strDate, format);

            outHour.set(creationDateTime.getHour());

            // get the comment length
            outCountAverage.setCount(1);
            outCountAverage.setAverage(text.length());

            context.write(outHour, outCountAverage);

        }
    }

    public static  class AverageReducer extends
            Reducer<IntWritable, CountAverageTuple,
                    IntWritable, CountAverageTuple> {

        private CountAverageTuple result = new CountAverageTuple();

        @Override
        protected void reduce(IntWritable key, Iterable<CountAverageTuple> values, Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple>.Context context) throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;

            // Iteration through all input values for this key
            for (CountAverageTuple val: values) {
                sum += val.getCount() * val.getAverage();
                count += val.getCount();
            }
            result.setCount(count);
            result.setAverage(sum / count);
            context.write(key, result);
        }
    }
}
