package org.example.stats;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.XMLParserUtil;

import java.io.IOException;
import java.util.Map;

public class WikipediaIndexer {
    public static class WikipediaExtractor extends Mapper <Object, Text, Text, Text> {
        private Text link = new Text();
        private Text outKey = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Map<String,String> parsed = XMLParserUtil.transformXmlToMap(value.toString());

            // Grab the necessary XML attributes
            String txt = parsed.get("Body");
            String postType = parsed.get("PostTypeId");
            String row_id = parsed.get("Id");

            // if the body is null, or the post is a question (1), skip
            if (txt == null || (postType != null && postType.equals("1"))) {
                return;
            }
            // Unescape the HTML because the SO data is escaped
            txt = StringEscapeUtils.unescapeHtml4(txt.toLowerCase());
            link.set(getWikipediaURL(txt));
            outKey.set(row_id);
            context.write(link, outKey);
        }

        private String getWikipediaURL(String txt) {
            // TODO!! implement me
            return txt;
        }
    }

    public static class Concatenator extends Reducer <Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for(Text id: values) {
                if (first) {
                    first = false;
                } else {
                    sb.append(" ");
                }
                sb.append(id.toString());
            }
            result.set(sb.toString());
            context.write(key, result);
        }
    }

}
