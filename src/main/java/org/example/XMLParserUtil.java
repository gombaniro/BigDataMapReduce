package org.example;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

public class XMLParserUtil {

    public static Map<String, String> transformXmlToMap(String xml) {
        Document doc = null;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            DocumentBuilder bldr = factory
                    .newDocumentBuilder();

            doc = bldr.parse(new ByteArrayInputStream(xml.getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        Map<String, String> map = new HashMap<String, String>();
        NamedNodeMap attributeMap = doc.getDocumentElement().getAttributes();

        for (int i = 0; i < attributeMap.getLength(); ++i) {
            Attr n = (Attr) attributeMap.item(i);

            map.put(n.getName(), n.getValue());
        }

        return map;
    }
}
