/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.lib.database.launcher.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * This class provides easier parsing of the configuration data in {@code launcher-config.xml}
 *
 * @author javatarz (Karun Japhet)
 * @author sanketsarang
 */
public class ConfigUtil {

    private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);
    private static final Map<String, Map<String, Map<String, List<String>>>> configDataMap = new HashMap<>();

    /**
     * Fetches configuration values from the specified document for the requested query if not cached already.
     *
     * @param documentName XML document to be parsed
     * @param query {@link XPath} valid query to be used to parse the document for results
     * @return {@link Map} with the key value pair from the XML
     */
    public static Map<String, List<String>> getConfigData(final String documentName, final String query) {
        Map<String, Map<String, List<String>>> documentData = configDataMap.get(documentName);
        if (documentData == null) {
            documentData = new HashMap<>();
            configDataMap.put(documentName, documentData);
        }

        Map<String, List<String>> queryData = documentData.get(query);
        if (queryData == null) {
            try {
                queryData = getDataFromConfFile(documentName, query);
                documentData.put(query, queryData);
            } catch (SAXException | IOException | XPathExpressionException | ParserConfigurationException ex) {
                final String message = "Error while attempting to read \"" + documentName + "\" for query \"" + query + "\"";

                logger.error(message, ex);
                throw new RuntimeException(message, ex);
            }
        }

        return queryData;
    }

    /**
     * Parses the XML configuration file specified by {@code documentName} and returns the result of the query as a parsed map
     *
     * @param documentName XML document to be parsed
     * @param query {@link XPath} valid query to be used to parse the document for results
     * @return {@link Map} with the key value pair from the XML
     *
     * @throws SAXException
     * @throws IOException
     * @throws XPathExpressionException
     * @throws ParserConfigurationException
     */
    private static Map<String, List<String>> getDataFromConfFile(final String documentName, final String query) throws SAXException, IOException, XPathExpressionException, ParserConfigurationException {
        logger.trace("Attempting to get config values from method \"" + documentName + "\" with query \"" + query + "\"");

        // Creating an XML parser
        final DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        // Setting the entity resolver so it finds the DTD correctly
        db.setEntityResolver((String publicId, String systemId) -> {
            if (systemId.contains(".dtd")) {
                final String[] fileParts = systemId.split(Pattern.quote(File.separator));
                return new InputSource(ClassLoader.getSystemResourceAsStream(fileParts[fileParts.length - 1]));
            }

            return null;
        });

        // Loading the document
        final Document doc = db.parse(ClassLoader.getSystemResourceAsStream(documentName));
        final XPath xPath = XPathFactory.newInstance().newXPath();

        // Executing the query
        final NodeList nodeList = (NodeList) xPath.compile(query).evaluate(doc, XPathConstants.NODESET);

        // Creating the result to be returned
        final Map<String, List<String>> elementDataMap = new HashMap<>();
        for (int i = 0; i < nodeList.getLength(); i++) {
            String key = "";
            final Node attributeNode = nodeList.item(i).getAttributes().getNamedItem("id");
            if (attributeNode != null) {
                key = attributeNode.getNodeValue();
            }

            List<String> dataList = elementDataMap.get(key);
            if (dataList == null) {
                dataList = new ArrayList<>();
            }

            final String value = nodeList.item(i).getFirstChild().getNodeValue();
            dataList.add(value);
            elementDataMap.put(key, dataList);
        }

        return elementDataMap;
    }
}
