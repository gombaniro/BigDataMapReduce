package org.example.organization;

import org.apache.hadoop.io.Text;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;

public class PostXMLUtil {

    private static final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

    public static String removeFlag(Text t) {
        return t.toString()
                .substring(1).trim();
    }

    public static String nestElements(String post, ArrayList<String> comments) throws ParserConfigurationException, IOException, SAXException, TransformerException {
        // Create the new documents to build the XML
        DocumentBuilder builder = dbf.newDocumentBuilder();
        Document doc = builder.newDocument();

        // Copy parent node to document
        Element postEl = getXMLElementFromString(post);
        Element toAddPostEl = doc.createElement("post");

        // copy the attributes of the original post element to the new one
        copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

        // For each comment , copy it tot hte "post" node
        for (String commentXML : comments) {
            Element commentEl = getXMLElementFromString(commentXML);
            Element toAddCommentEl = doc.createElement("comments");

            //Copy the attributes of the original comment element to
            // the new one
            copyAttributesToElement(commentEl.getAttributes(), toAddCommentEl);

            // Add the copied comment to the post element
            toAddPostEl.appendChild(toAddCommentEl);
        }
        // Add the post element to the document
        doc.appendChild(toAddPostEl);
        return transformDocumentToString(doc);
    }

    public static Element getXMLElementFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        // Create a new document builder
        DocumentBuilder builder = dbf.newDocumentBuilder();
        return builder.parse(new InputSource(
                new StringReader(xml)
        )).getDocumentElement();
    }

    public static void copyAttributesToElement(NamedNodeMap attributes,
                                               Element element) {
        // For each attribute , copy it to the element
        for (int i = 0; i < attributes.getLength(); i++) {
            Attr toCopy = (Attr) attributes.item(i);
            element.setAttribute(toCopy.getName(), toCopy.getValue());
        }
    }

    public static String transformDocumentToString(Document doc) throws TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "Yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(
                writer
        ));
        return writer.getBuffer().toString().replaceAll("\n|\r", "");
    }
}
