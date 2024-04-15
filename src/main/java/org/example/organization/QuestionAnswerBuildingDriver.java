package org.example.organization;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.ArrayList;

public class QuestionAnswerBuildingDriver {



    public static class PostCommentMapper extends
            Mapper<Object, Text, Text, Text> {
        private  Text outKey = new Text();
        private  Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // Parse the post/comment XML hierarchy into an Element
            try {
                Element post = PostXMLUtil.getXMLElementFromString(value.toString());
                int postType = Integer.parseInt(post.getAttribute("PostTypeId"));

                // If postType is 1, it is a question
                if (postType == 1) {
                    outKey.set(post.getAttribute("id"));
                    outValue.set("Q" + value);
                } else {
                    // Else , it is an answer
                    outKey.set(post.getAttribute("ParentId"));
                    outValue.set("A" + value);
                }
                context.write(outKey, outValue);


            } catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            } catch (SAXException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class QuestionAnswerReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        private String question = null;
        private ArrayList<String> answers = new ArrayList<>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

            // Reset variables
            question = null;
            answers.clear();

            // For each input value
            for(Text t: values) {
                // if this is the post record, store it , minus the flag
                if (t.charAt(0) == 'Q') {
                    question = PostXMLUtil.removeFlag(t);
                } else {
                    // Else, it is a comment record. Add it to the list, minus the flag
                    answers.add(PostXMLUtil.removeFlag(t));
                }
            }
            // If post is not null
            if (question != null) {
                // nest the comments underneath the post element
                try {
                    String postWithCommentChildren = PostXMLUtil.nestElements(question, answers);

                    // Write out the XML
                    context.write(new Text(postWithCommentChildren), NullWritable.get());
                } catch (ParserConfigurationException e) {
                    throw new RuntimeException(e);
                } catch (SAXException e) {
                    throw new RuntimeException(e);
                } catch (TransformerException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
