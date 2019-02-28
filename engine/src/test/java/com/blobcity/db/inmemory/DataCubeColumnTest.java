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
import com.blobcity.db.lang.Operators;
import com.blobcity.db.memory.records.XmlRecord;
import com.blobcity.db.olap.DataCubeColumn;
import com.blobcity.db.memory.old.MemoryTable;
import com.blobcity.db.memory.old.MemoryTableStore;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
import org.junit.Ignore;
import org.junit.Test;
import org.testng.annotations.*;

/**
 * This is the default class comment, please change it! If you're committing this, your merge WILL NOT BE ACCEPTED. You
 * have been warned!
 *
 * @author sanketsarang
 */
@Ignore
public class DataCubeColumnTest {

//    DataCubeColumn cubeCol, cubeCol1, cubeCol5;
//    MemoryTable tbl, tbl5;
//
//    public DataCubeColumnTest() {
//    }
//
//    @BeforeClass
//    public void before() {
//        System.out.println("in before");
//        cubeCol = new DataCubeColumn();
//        tbl = new MemoryTable();
//
//        try {
//            tbl.setName("tbl1");
//            tbl.insert("pk1", new JSONObject("{\"col1\": \"123\", \"col2\": \"456\"}"));
//            tbl.insert("pk2",  new JSONObject("{\"col1\": \"234\", \"col3\": \"789\"}"));
//            tbl.insert("pk3",  new JSONObject("{\"col1\": \"456\", \"col2\": \"656\"}"));
//            tbl.insert("pk4",  new JSONObject("{\"col1\": \"678\", \"col3\": \"985\"}"));
//            tbl.insert("pk5",  new JSONObject("{\"col1\": \"899\", \"col2\": \"345\"}"));
//            MemoryTableStore.createStore();
//            MemoryTableStore.add(tbl);
//
//            cubeCol.setName("tbl1.col1");
//            cubeCol.setColViewableName("col1");
//            cubeCol.setColInternalName("1");
//            cubeCol.setTableName("tbl1");
//
//            cubeCol.insert("123", "pk1");
//            cubeCol.insert("234", "pk2");
//            cubeCol.insert("456", "pk3");
//            cubeCol.insert("678", "pk4");
//            cubeCol.insert("899", "pk5");
//
//        } catch (OperationException ex) {
//            Logger.getLogger(DataCubeColumnTest.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//    @Test
//    public void somethingElse(){
//
//    }
//
//    @Test
//    public void testConstructor() {
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
//        System.out.println("Testing XmlRecord.asJson()");
//        JSONObject jsonObject = xmlRecord.asJson();
//        Assert.assertNotNull(jsonObject);
//
//        System.out.println("XML to JSON: " + jsonObject.toString());
//    }
//
//    @Test
//    public void testCase1() {
//        cubeCol = new DataCubeColumn();
//        tbl = new MemoryTable();
//        cubeCol.setName("testCubeCol");
//        System.out.println("testCase1 cubeCol name: " + cubeCol.getName());
//
//    }
//
//    //@Test
//    public void testCase2() throws OperationException {
//        System.out.println("inserting: testKey, testVal");
////            cubeCol = new DataCubeColumn();
//            cubeCol.insert("123", "pk1");
//            cubeCol.insert("123", "pk2");
//            cubeCol.insert("2000", "pk2");
//            cubeCol.insert("2000", "pk4");
//            cubeCol.insert("356", "pk5");
//    }
//
//    //@Test
//    public void testCase3() throws OperationException {
//        System.out.println("save: testCol, testVal");
//
////            cubeCol = new DataCubeColumn();
////        tbl = new MemoryTable();
//             cubeCol.insert("1", "pk1");
////            cubeCol.save("testkey1", "testkey2", "2");
//
//    }
//
//    @Test
//    public void testCase4() throws OperationException {
//        System.out.println("testCase4 insert: testCol, testVal");
//
//            cubeCol1 = new DataCubeColumn();
//            cubeCol.setName("tbl1.col1");
//            cubeCol.setColViewableName("col1");
//            cubeCol.setColInternalName("1");
//            cubeCol.setTableName("tbl1");
////        tbl = new MemoryTable();
//             cubeCol1.insert("string1", "pk1");
//             cubeCol1.insert("string1", "pk2");
//             cubeCol1.insert("string2", "pk3");
//             cubeCol1.insert("string4", "pk4");
//             cubeCol1.insert("string5", "pk8");
////            cubeCol.save("testkey1", "testkey2", "2");
//
//    }
//
////    @Test(threadPoolSize = 3, invocationCount = 100, timeOut = 10000)
//    @Test
//    public void testCase5() {
//
//        System.out.println("testCase5: testCol, testVal");
//
//        cubeCol5 = new DataCubeColumn();
//        tbl5 = new MemoryTable();
//
//        try {
//            tbl5.setName("tbl5");
//            tbl5.insert("pk1", new JSONObject("{\"col1\": \"123\", \"col2\": \"456\"}"));
//            tbl5.insert("pk2", new JSONObject("{\"col1\": \"234\", \"col3\": \"789\"}"));
//            tbl5.insert("pk3", new JSONObject("{\"col1\": \"456\", \"col2\": \"656\"}"));
//            tbl5.insert("pk4", new JSONObject("{\"col1\": \"678\", \"col3\": \"985\"}"));
//            tbl5.insert("pk5", new JSONObject("{\"col1\": \"899\", \"col2\": \"345\"}"));
//
//            MemoryTableStore.createStore();
//            MemoryTableStore.add(tbl5);
//
//            cubeCol5.setName("tbl5.col1");
//            cubeCol5.setTableName("tbl5");
//            cubeCol5.setColViewableName("col1");
//            cubeCol5.setColInternalName("1");
//
//            cubeCol5.insert("123", "pk1");
//            cubeCol5.insert("234", "pk2");
//            cubeCol5.insert("456", "pk3");
//            cubeCol5.insert("678", "pk4");
//            cubeCol5.insert("899", "pk5");
//
//            List<Object> records = new ArrayList(tbl5.getAllRecordsInTbl());
//            for (Iterator<Object> iter = records.iterator(); iter.hasNext();) {
//                Object record = iter.next();
//                System.out.println("record: " + record.toString());
//            }
//        } catch (OperationException ex) {
//            Logger.getLogger(DataCubeColumnTest.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        try {
//            List<Object> pks = cubeCol5.search(Operators.LT, "500");
//            List<Object> records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items in col1 LT 500: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while ((iter != null) && iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.GT, "500");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 GT 500: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.EQ, "456");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 EQ 456: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.NEQ, "123");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NEQ 123: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.LTEQ, "789");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 LTEQ 789: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.LTEQ, "800");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 LTEQ 800: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.GTEQ, "456");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 GTEQ 456: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.GTEQ, "600");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 GTEQ 600: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.IN, "400", "700");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 IN 400, 700: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.IN, "456", "789");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 IN 456 789: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.NOT_IN, "456", "789");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NOT_IN 456 789: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.NOT_IN, "400", "700");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NOT_IN 400 700: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.BETWEEN, "400", "700");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 BETWEEN 400, 700: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.BETWEEN, "456", "789");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 BETWEEN 456 789: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.NOT_BETWEEN, "456", "789");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NOT_BETWEEN 456 789: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//
//            pks = cubeCol5.search(Operators.NOT_BETWEEN, "400", "700");
//            records = tbl5.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NOT_BETWEEN 400 700: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    JSONObject jsonObj = (JSONObject)(iter.next());
//                    if(jsonObj != null) System.out.println(jsonObj.toString());
//                }
//            }
//        } catch (Exception e) {
//            System.out.println("Exception caught ");
//            e.printStackTrace();
//        }
//    }
}
