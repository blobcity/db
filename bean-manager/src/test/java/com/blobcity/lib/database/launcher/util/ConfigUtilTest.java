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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Test class for {@link ConfigUtil}
 *
 * @author javatarz (Karun Japhet)
 */
public class ConfigUtilTest {

    private static final Logger logger = LoggerFactory.getLogger(ConfigUtilTest.class);

    /**
     * Test for the {@code Factories} tag in a config using {@link ConfigUtil#getConfigData(java.lang.String, java.lang.String)}
     *
     * @throws ParserConfigurationException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws SAXException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws XPathExpressionException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws IOException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws ClassNotFoundException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     */
    @Test
    public void testConfigFactories() throws ParserConfigurationException, SAXException, XPathExpressionException, IOException, ClassNotFoundException {
        logger.info("ConfigUtil.getConfigValues(String, String): Factories");

        final String documentName = "test.xml";
        final String query = "/config/factories/name";

        final Map<String, List<String>> expResult = new HashMap<>();
        expResult.put("key1", Arrays.asList(new String[]{"Value 1", "Value 3"}));
        expResult.put("key2", Arrays.asList(new String[]{"Value 5"}));
        final Map<String, List<String>> result = ConfigUtil.getConfigData(documentName, query);

        assertEquals(expResult, result);
    }

    /**
     * Test for the {@code Ports} tag in a config using {@link ConfigUtil#getConfigData(java.lang.String, java.lang.String)}
     *
     * @throws ParserConfigurationException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws SAXException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws XPathExpressionException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws IOException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws ClassNotFoundException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     */
    @Test
    public void testConfigPorts() throws ParserConfigurationException, SAXException, XPathExpressionException, IOException, ClassNotFoundException {
        logger.info("ConfigUtil.getConfigValues(String, String): Ports");

        final String documentName = "test.xml";
        final String query = "/config/ports/port";

        final Map<String, List<String>> expResult = new HashMap<>();
        expResult.put("app1", Arrays.asList(new String[]{"1"}));
        expResult.put("app2", Arrays.asList(new String[]{"2"}));
        final Map<String, List<String>> result = ConfigUtil.getConfigData(documentName, query);

        assertEquals(expResult, result);
    }

    /**
     * Test for the {@code Hosts} tag in a config using {@link ConfigUtil#getConfigData(java.lang.String, java.lang.String)}
     *
     * @throws ParserConfigurationException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws SAXException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws XPathExpressionException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws IOException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws ClassNotFoundException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     */
    @Test
    public void testConfigHosts() throws ParserConfigurationException, SAXException, XPathExpressionException, IOException, ClassNotFoundException {
        logger.info("ConfigUtil.getConfigValues(String, String): Hosts");

        final String documentName = "test.xml";
        final String query = "/config/hosts/host";

        final Map<String, List<String>> expResult = new HashMap<>();
        expResult.put("app1", Arrays.asList(new String[]{"http://blobcity.com"}));
        expResult.put("app2", Arrays.asList(new String[]{"http://blobcity.org"}));
        final Map<String, List<String>> result = ConfigUtil.getConfigData(documentName, query);

        assertEquals(expResult, result);
    }

    /**
     * Test for the {@code Resources} tag in a config using {@link ConfigUtil#getConfigData(java.lang.String, java.lang.String)}
     *
     * @throws ParserConfigurationException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws SAXException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws XPathExpressionException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws IOException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws ClassNotFoundException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     */
    @Test
    public void testConfigResources() throws ParserConfigurationException, SAXException, XPathExpressionException, IOException, ClassNotFoundException {
        logger.info("ConfigUtil.getConfigValues(String, String): Resources");

        final String documentName = "test.xml";
        final String query = "/config/resources/package";

        final Map<String, List<String>> expResult = new HashMap<>();
        expResult.put("pkg1", Arrays.asList(new String[]{"val-1.0", "val-1.5"}));
        expResult.put("pkg2", Arrays.asList(new String[]{"val-2"}));
        final Map<String, List<String>> result = ConfigUtil.getConfigData(documentName, query);

        assertEquals(expResult, result);
    }

    /**
     * Test for the {@code Inject} tag in a config using {@link ConfigUtil#getConfigData(java.lang.String, java.lang.String)}
     *
     * @throws ParserConfigurationException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws SAXException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws XPathExpressionException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws IOException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     * @throws ClassNotFoundException refer to method documentation for {@link ConfigUtil#getConfigValues(java.lang.String, java.lang.String)}
     */
    @Test
    public void testConfigInject() throws ParserConfigurationException, SAXException, XPathExpressionException, IOException, ClassNotFoundException {
        logger.info("ConfigUtil.getConfigValues(String, String): Inject");

        final String documentName = "test.xml";
        final String query = "/config/inject/class";

        final Map<String, List<String>> expResult = new HashMap<>();
        expResult.put("", Arrays.asList(new String[]{"val1", "val2"}));
        final Map<String, List<String>> result = ConfigUtil.getConfigData(documentName, query);

        assertEquals(expResult, result);
    }
}
