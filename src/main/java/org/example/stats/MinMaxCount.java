package org.example.stats;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.XMLParserUtil;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class MinMaxCount {
    public static void main(String[] args) {

    }

    public static class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
        private final static DateTimeFormatter format =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        // Our output key and value Writable
        private final Text outUserId = new Text();
        private final MinMaxCountTuple outTuple = new MinMaxCountTuple();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = XMLParserUtil.transformXmlToMap(value.toString());
            // Grab "creationDate" field since it is what we are finding
            String strDate = parsed.get("creationDate");
            // Grab the "UserID" since it is what we are grouping by
            String userId = parsed.get("UserId");

            LocalDate creationDate = LocalDate.parse(strDate, format);
            // Set the minimum and maximum date values to the creationDate
            outTuple.setMin(creationDate);
            outTuple.setMax(creationDate);

            // Set the comment count to 1
            outTuple.setCount(1);

            // Set our user ID as the output key
            outUserId.set(userId);
            // Write out the hour and the average comment length
            context.write(outUserId, outTuple);
        }
    }

    public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

        // Our output value Writable
        private MinMaxCountTuple result = new MinMaxCountTuple();

        @Override
        protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>.Context context) throws IOException, InterruptedException {
            // Initialize our result
            result.setMin(null);
            result.setMax(null);
            result.setCount(0);
            int sum = 0;

            // Iterate through all input values for this key
            for(MinMaxCountTuple val : values) {
                // If the value's min is less than the result's min
                // Set the result's min to value's
                if (result.getMin() == null || val.getMin().compareTo(result.getMin()) < 0) {
                    result.setMin(val.getMin());
                }
                // If the value's max is more than the result's max
                // Set the result's max to value's
                if (result.getMax() == null || val.getMax().compareTo(result.getMax()) < 0) {
                    result.setMax(val.getMax());
                }
                sum += val.getCount();
            }
            // Set our Count to the  number of input values
            result.setCount(sum);
            context.write(key, result);
        }
    }
























}
