package org.example.organization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.example.XMLParserUtil;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class PostCommentBuildingDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PostCommentHierarchy");
        job.setJarByClass(PostCommentBuildingDriver.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PostMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentMapper.class);
        job.setReducerClass(PostCommentHierarchyReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }

    public static class PostMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = XMLParserUtil.transformXmlToMap(value.toString());

            // The foreign join key is the post ID
            outKey.set(parsed.get("Id"));

            //Flag this record for the reducer and then output
            outKey.set("P" + value);
            context.write(outKey, outValue);

        }
    }

    public static class CommentMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = XMLParserUtil.transformXmlToMap(value.toString());

            // The foreign join key is the post ID
            outKey.set(parsed.get("PostId"));

            //Flag this record for the reducer and then output
            outKey.set("C" + value);
            context.write(outKey, outValue);
        }
    }

    public static class PostCommentHierarchyReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        private final ArrayList<String> comments = new ArrayList<>();
        private final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        private String post;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            // Reset variables
            post = null;
            comments.clear();
            // for each input value
            for (Text t : values) {
                // If this is the post record, store it , minus the flag
                if (t.charAt(0) == 'P') {
                    post = PostXMLUtil.removeFlag(t);
                } else {
                    // Else, it is a comment record. Add it to the list, minus the flag
                    comments.add(PostXMLUtil.removeFlag(t));
                }
            }
            // If there are no comments, the comment list will be simply be empty.

            // If post is not null, combine post with its comments
            if (post != null) {
                // nest the comments underneath the post element
                String postWithCommentsChildren = null;
                try {
                    postWithCommentsChildren = PostXMLUtil.nestElements(post, comments);
                } catch (ParserConfigurationException | SAXException | TransformerException e) {
                    throw new RuntimeException(e);
                }
                // write out the XML
                context.write(new Text(postWithCommentsChildren), NullWritable.get());
            }

        }
    }
}
