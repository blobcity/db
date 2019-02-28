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

package com.blobcity.db.inmemory;

import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.memory.records.XmlRecord;
import junit.framework.Assert;
import org.json.JSONObject;
import org.junit.Ignore;

import java.util.Map;

/**
 * @author sanketsarang
 */
@Ignore
public class XmlRecordTest {

//    @org.testng.annotations.BeforeClass
//    public void before() {
//        System.out.println("Running XmlRecordTest");
//    }
//
//    @org.testng.annotations.AfterClass
//    public void after() {
//
//    }
//
//    @org.testng.annotations.Test
//    public void xmlRecordPositiveTest() {
//        System.out.println("Testing creation of an XML record");
//        XmlRecord xmlRecord = new XmlRecord("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
//                "<current_observation>\n" +
//                "\n" +
//                "<credit>NOAA's National Weather Service</credit>\n" +
//                "<credit_URL>http://weather.gov/</credit_URL>\n" +
//                "\n" +
//                "<image>\n" +
//                "  <url>http://weather.gov/images/xml_logo.gif</url>\n" +
//                "  <title>NOAA's National Weather Service</title>\n" +
//                "  <link>http://weather.gov</link>\n" +
//                "</image>\n" +
//                "\n" +
//                "<location>New York/John F. Kennedy Intl Airport, NY</location>\n" +
//                "<station_id>KJFK</station_id>\n" +
//                "<latitude>40.66</latitude>\n" +
//                "<longitude>-73.78</longitude>\n" +
//                "<observation_time_rfc822>Mon, 11 Feb 2008 06:51:00 -0500 EST\n" +
//                "</observation_time_rfc822>\n" +
//                "\n" +
//                "<weather>A Few Clouds</weather>\n" +
//                "<temp_f>11</temp_f>\n" +
//                "<temp_c>-12</temp_c>\n" +
//                "<relative_humidity>36</relative_humidity>\n" +
//                "<wind_dir>West</wind_dir>\n" +
//                "<wind_degrees>280</wind_degrees>\n" +
//                "<wind_mph>18.4</wind_mph>\n" +
//                "<wind_gust_mph>29</wind_gust_mph>\n" +
//                "<pressure_mb>1023.6</pressure_mb>\n" +
//                "<pressure_in>30.23</pressure_in>\n" +
//                "<dewpoint_f>-11</dewpoint_f>\n" +
//                "<dewpoint_c>-24</dewpoint_c>\n" +
//                "<windchill_f>-7</windchill_f>\n" +
//                "<windchill_c>-22</windchill_c>\n" +
//                "<visibility_mi>10.00</visibility_mi>\n" +
//                "\n" +
//                "<icon_url_base>http://weather.gov/weather/images/fcicons/</icon_url_base>\n" +
//                "<icon_url_name>nfew.jpg</icon_url_name>\n" +
//                "<disclaimer_url>http://weather.gov/disclaimer.html</disclaimer_url>\n" +
//                "<copyright_url>http://weather.gov/disclaimer.html</copyright_url>\n" +
//                "\n" +
//                "</current_observation>");
//
//        System.out.println("Testing XmlRecord.getRecord()");
//        JSONObject jsonObject = xmlRecord.asJson();
//        Assert.assertNotNull(jsonObject);
//        System.out.println("XML to Json: " + jsonObject.toString());
//    }
//
//    @org.testng.annotations.Test
//    public void xmlRecordArrayTest() {
//        System.out.println("Testing for XML arrays");
//        XmlRecord xmlRecord = new XmlRecord("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
//                "<breakfast_menu>\n" +
//                "    <food>\n" +
//                "        <name>Belgian Waffles</name>\n" +
//                "        <price>$5.95</price>\n" +
//                "        <description>Our famous Belgian Waffles with plenty of real maple syrup</description>\n" +
//                "        <calories>650</calories>\n" +
//                "    </food>\n" +
//                "    <food>\n" +
//                "        <name>French Toast</name>\n" +
//                "        <price>$4.50</price>\n" +
//                "        <description>Thick slices made from our homemade sourdough bread</description>\n" +
//                "        <calories>600</calories>\n" +
//                "    </food>\n" +
//                "    <food>\n" +
//                "        <name>Homestyle Breakfast</name>\n" +
//                "        <price>$6.95</price>\n" +
//                "        <description>Two eggs, bacon or sausage, toast, and our ever-popular hash browns</description>\n" +
//                "        <calories>950</calories>\n" +
//                "    </food>\n" +
//                "</breakfast_menu>");
//
//        JSONObject jsonObject = xmlRecord.asJson();
//        Assert.assertNotNull(jsonObject);
//        System.out.println("XML to Json: " + jsonObject.toString());
//
//    }
//
//    @org.testng.annotations.Test
//    public void xmlRecordArrayWithAttributesTest() {
//        System.out.println("Testing for XML arrays with element attributes");
//        XmlRecord xmlRecord = new XmlRecord("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
//                "<breakfast_menu>\n" +
//                "    <food type=\"italian\">\n" +
//                "        <name>Belgian Waffles</name>\n" +
//                "        <price>$5.95</price>\n" +
//                "        <description>Our famous Belgian Waffles with plenty of real maple syrup</description>\n" +
//                "        <calories>650</calories>\n" +
//                "    </food>\n" +
//                "    <food type=\"french\">\n" +
//                "        <name>French Toast</name>\n" +
//                "        <price>$4.50</price>\n" +
//                "        <description>Thick slices made from our homemade sourdough bread</description>\n" +
//                "        <calories>600</calories>\n" +
//                "    </food>\n" +
//                "    <food type=\"general\">\n" +
//                "        <name>Homestyle Breakfast</name>\n" +
//                "        <price>$6.95</price>\n" +
//                "        <description>Two eggs, bacon or sausage, toast, and our ever-popular hash browns</description>\n" +
//                "        <calories>950</calories>\n" +
//                "    </food>\n" +
//                "</breakfast_menu>");
//
//        JSONObject jsonObject = xmlRecord.asJson();
//        Assert.assertNotNull(jsonObject);
//        System.out.println("XML to Json: " + jsonObject.toString());
//    }
//
//    @org.testng.annotations.Test
//    public void xmlRecordWithoutXmlTagTest() {
//        System.out.println("Testing for XML without opening XML tag");
//        XmlRecord xmlRecord = new XmlRecord("<breakfast_menu>\n" +
//                "    <food type=\"italian\">\n" +
//                "        <name>Belgian Waffles</name>\n" +
//                "        <price>$5.95</price>\n" +
//                "        <description>Our famous Belgian Waffles with plenty of real maple syrup</description>\n" +
//                "        <calories>650</calories>\n" +
//                "    </food>\n" +
//                "    <food type=\"french\">\n" +
//                "        <name>French Toast</name>\n" +
//                "        <price>$4.50</price>\n" +
//                "        <description>Thick slices made from our homemade sourdough bread</description>\n" +
//                "        <calories>600</calories>\n" +
//                "    </food>\n" +
//                "    <food type=\"general\">\n" +
//                "        <name>Homestyle Breakfast</name>\n" +
//                "        <price>$6.95</price>\n" +
//                "        <description>Two eggs, bacon or sausage, toast, and our ever-popular hash browns</description>\n" +
//                "        <calories>950</calories>\n" +
//                "    </food>\n" +
//                "</breakfast_menu>");
//
//        JSONObject jsonObject = xmlRecord.asJson();
//        Assert.assertNotNull(jsonObject);
//        System.out.println("XML to Json: " + jsonObject.toString());
//    }
//
//
//    @org.testng.annotations.Test
//    public void xmlRecordWithoutRootElement() {
//        System.out.println("Testing for XML without root element");
//        XmlRecord xmlRecord = new XmlRecord("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
//                "<food type=\"italian\">\n" +
//                "    <name>Belgian Waffles</name>\n" +
//                "    <price>$5.95</price>\n" +
//                "    <description>Our famous Belgian Waffles with plenty of real maple syrup</description>\n" +
//                "    <calories>650</calories>\n" +
//                "</food>\n" +
//                "<food type=\"french\">\n" +
//                "<name>French Toast</name>\n" +
//                "<price>$4.50</price>\n" +
//                "<description>Thick slices made from our homemade sourdough bread</description>\n" +
//                "<calories>600</calories>\n" +
//                "</food>\n" +
//                "<food type=\"general\">\n" +
//                "<name>Homestyle Breakfast</name>\n" +
//                "<price>$6.95</price>\n" +
//                "<description>Two eggs, bacon or sausage, toast, and our ever-popular hash browns</description>\n" +
//                "<calories>950</calories>\n" +
//                "</food>");
//
//        JSONObject jsonObject = xmlRecord.asJson();
//        Assert.assertNotNull(jsonObject);
//        System.out.println("XML to Json: " + jsonObject.toString());
//    }
//
//    @org.testng.annotations.Test
//    public void xmlRecordArrayPlusKeysWithoutRootTest() {
//        System.out.println("Testing for XML containing array plus keys without root element");
//        XmlRecord xmlRecord = new XmlRecord("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
//                "<food type=\"italian\">\n" +
//                "    <name>Belgian Waffles</name>\n" +
//                "    <price>$5.95</price>\n" +
//                "    <description>Our famous Belgian Waffles with plenty of real maple syrup</description>\n" +
//                "    <calories>650</calories>\n" +
//                "</food>\n" +
//                "<food type=\"french\">\n" +
//                "<name>French Toast</name>\n" +
//                "<price>$4.50</price>\n" +
//                "<description>Thick slices made from our homemade sourdough bread</description>\n" +
//                "<calories>600</calories>\n" +
//                "</food>\n" +
//                "<food type=\"general\">\n" +
//                "<name>Homestyle Breakfast</name>\n" +
//                "<price>$6.95</price>\n" +
//                "<description>Two eggs, bacon or sausage, toast, and our ever-popular hash browns</description>\n" +
//                "<calories>950</calories>\n" +
//                "</food>\n" +
//                "<open>11:00 am</open>\n" +
//                "<close>12:00 am</close>");
//
//        JSONObject jsonObject = xmlRecord.asJson();
//        Assert.assertNotNull(jsonObject);
//        System.out.println("XML to Json: " + jsonObject.toString());
//    }
//
//    @org.testng.annotations.Test
//    public void xmlRecordArrayPlusKeyTest() {
//        System.out.println("Testing for XML containing array plus keys");
//        XmlRecord xmlRecord = new XmlRecord("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
//                "<restaurant>\n" +
//                "    <food type=\"italian\">\n" +
//                "        <name>Belgian Waffles</name>\n" +
//                "        <price>$5.95</price>\n" +
//                "        <description>Our famous Belgian Waffles with plenty of real maple syrup</description>\n" +
//                "        <calories>650</calories>\n" +
//                "    </food>\n" +
//                "    <food type=\"french\">\n" +
//                "        <name>French Toast</name>\n" +
//                "        <price>$4.50</price>\n" +
//                "        <description>Thick slices made from our homemade sourdough bread</description>\n" +
//                "        <calories>600</calories>\n" +
//                "    </food>\n" +
//                "    <food type=\"general\">\n" +
//                "        <name>Homestyle Breakfast</name>\n" +
//                "        <price>$6.95</price>\n" +
//                "        <description>Two eggs, bacon or sausage, toast, and our ever-popular hash browns</description>\n" +
//                "        <calories>950</calories>\n" +
//                "    </food>\n" +
//                "    <open>11:00 am</open>\n" +
//                "    <close>12:00 am</close>\n" +
//                "</restaurant>");
//
//        JSONObject jsonObject = xmlRecord.asJson();
//        Assert.assertNotNull(jsonObject);
//        System.out.println("XML to Json: " + jsonObject.toString());
//    }
}
