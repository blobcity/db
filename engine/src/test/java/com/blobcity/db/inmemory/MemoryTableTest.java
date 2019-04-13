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
import com.blobcity.db.memory.old.MemoryTable;
import com.blobcity.db.memory.old.MemoryTableStore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.json.JSONObject;
import org.junit.Test;
import org.junit.BeforeClass;
import org.testng.annotations.*;

/**
 * //TODO: Improve and check tests with new in-memory table implementation
 *
 * This is the default class comment, please change it! If you're committing this, your merge WILL NOT BE ACCEPTED. You
 * have been warned!
 *
 * @author sanketsarang
 */
@org.junit.Ignore
public class MemoryTableTest {

//    MemoryTable tbl;
//
//    public MemoryTableTest() {
//    }
//
//    @BeforeClass
//    public void before() {
//
//        tbl = new MemoryTable();
//        System.out.println("in before");
//
//        tbl.setName("testTable");
//        tbl.addColToIndex("col1");
//        tbl.addColToIndex("col2");
//
//        try {
//            MemoryTableStore.createStore();
//            MemoryTableStore.add(tbl);
//
//            tbl.insert("pk1", new JSONObject("{\"col1\": \"123\", \"col2\": \"456\"}"));
//            tbl.insert("pk2", new JSONObject("{\"col1\": \"234\", \"col3\": \"789\"}"));
//            tbl.insert("pk3", new JSONObject("{\"col1\": \"456\", \"col2\": \"656\"}"));
//            tbl.insert("pk4", new JSONObject("{\"col1\": \"678\", \"col3\": \"985\"}"));
//            tbl.insert("pk5", new JSONObject("{\"col1\": \"899\", \"col2\": \"345\"}"));
//
//
//        } catch (OperationException e) {
//            System.out.println("insert failed: " + e.getMessage());
//        }
//    }
//
//    @Test
//    public void testCase1() {
//        System.out.println("testCase1: " + tbl.getName());
//
//        List<Object> records = new ArrayList(tbl.getAllRecordsInTbl());
//        for (Iterator<Object> iter = records.iterator(); iter.hasNext();) {
//            Object record = iter.next();
//            System.out.println("record: " + record.toString());
//        }
//
//    }
//
//    @Test
//    public void testCase2() {
//        System.out.println("testCase2: " + tbl.getName());
//
//        List<Object> keys = new ArrayList<>();
//        keys.add("pk1");
//        keys.add("pk4");
//        List<Object> records = tbl.getAllRecords(keys);
//        for (ListIterator<Object> iter = records.listIterator(); iter.hasNext();) {
//            Object record = iter.next();
//            System.out.println("record: " + record.toString());
//        }
//
//        Object record = tbl.getRecord("pk5");
//        System.out.println("record-pk5: " + record.toString());
//    }
//
//    @Test
//    public void testCase3() throws OperationException {
//        System.out.println("testCase3: " + tbl.getName());
//
//        tbl.save("pk2", new JSONObject("{\"col1\": \"2222\", \"col3\": \"7777\"}"));
//        List<Object> records = new ArrayList(tbl.getAllRecordsInTbl());
//        for (ListIterator<Object> iter = records.listIterator(); iter.hasNext();) {
//            Object record = iter.next();
//            System.out.println("record: " + record.toString());
//        }
//
//        Object record = tbl.getRecord("pk2");
//        System.out.println("record-pk2: " + record.toString());
//
//    }
//
//    @Test
//    public void testCase4() {
//        System.out.println("testCase4: " + tbl.getName());
//
//        tbl.remove("pk2");
//        List<Object> records = new ArrayList(tbl.getAllRecordsInTbl());
//        for (ListIterator<Object> iter = records.listIterator(); iter.hasNext();) {
//            Object record = iter.next();
//            System.out.println("record: " + record.toString());
//        }
//
//        Object record = tbl.getRecord("pk2");
//        if (record != null) {
//            System.out.println("record-pk2: " + record.toString());
//        }
//
//    }
//
//    @Test
//    public void testCase5() throws OperationException {
//        System.out.println("testCase5: " + tbl.getName());
//
//        String jsonStr = "{\n" +
//"    \"name\": \"Ashish Lal <ashish.lal@blobcity.com>\",\n" +
//"    \"address\": {\n" +
//"        \"street\": \"123 St\",\n" +
//"        \"pincode\": \"400111\"\n" +
//"    },\n" +
//"    \"about\": {\n" +
//"        \"desc\": \"software\",\n" +
//"        \"details\": {\n" +
//"            \"key1\": \"val1\",\n" +
//"            \"key2\": \"val2\"\n" +
//"        }\n" +
//"    }\n" +
//"}";
//
//        tbl.insert("pk10", new JSONObject(jsonStr));
//
//        List<Object> records = new ArrayList(tbl.getAllRecordsInTbl());
//        for (ListIterator<Object> iter = records.listIterator(); iter.hasNext();) {
//            Object record = iter.next();
//            System.out.println("record: " + record.toString());
//        }
//
//        try {
//            List<Object> pks = tbl.search(Operators.EQ, "address.pincode", "400111");
//            List<Object> records1 = tbl.getAllRecords(pks);
//            if (records1 != null) {
//                System.out.println("num items in address.pincode = 4001111: " + records1.size());
//                Iterator iter = records1.iterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//        } catch (OperationException e) {
//            System.out.println("test failed: " + e.getMessage());
//        }
//    }
//
//    @Test
//    public void testCase6() throws OperationException {
//        System.out.println("testCase6: " + tbl.getName());
//
//        String jsonStr =  "{\"markers\": [\n" +
//"		{\n" +
//"			\"homeTeam\":\"Lawrence Library\",\n" +
//"			\"awayTeam\":\"LUGip\",\n" +
//"		},\n" +
//"                {\n" +
//"			\"homeTeam\":\"Lawrence Library\",\n" +
//"			\"awayTeam\":\"LUGip\",\n" +
//"		},\n" +
//"		{\n" +
//"			\"homeTeam\":\"Hamilton Library\",\n" +
//"			\"awayTeam\":\"LUGip HW SIG\",\n" +
//"		},\n" +
//"		{\n" +
//"			\"homeTeam\":\"Applebees\",\n" +
//"			\"awayTeam\":\"After LUPip Mtg Spot\",\n" +
//"		},\n" +
//"] }";
//
//        tbl.insert("pk11", new JSONObject(jsonStr));
//
//        List<Object> records = new ArrayList(tbl.getAllRecordsInTbl());
//        for (ListIterator<Object> iter = records.listIterator(); iter.hasNext();) {
//            Object record = iter.next();
//            System.out.println("record: " + record.toString());
//        }
//
//        try {
//            List<Object> pks = tbl.search(Operators.EQ, "markers.homeTeam", "Hamilton Library");
//            List<Object> records1 = tbl.getAllRecords(pks);
//            if (records1 != null) {
//                System.out.println("num items in markers.homeTeam = Hamilton Library: " + records1.size());
//                ListIterator iter = records1.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//        } catch (OperationException e) {
//            System.out.println("test failed: " + e.getMessage());
//        }
//    }
//
//    //@Test
//    public void testCase7() {
//        System.out.println("testCase7: " + tbl.getName());
//        try {
//            List<Object> pks = tbl.search(Operators.LT, "col1", "500");
//            List<Object> records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items in col1 LT 500: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.GT, "col1", "500");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 GT 500: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.EQ, "col1", "456");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 EQ 456: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.NEQ, "col1", "123");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NEQ 123: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.LTEQ, "col1", "789");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 LTEQ 789: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.LTEQ, "col1", "800");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 LTEQ 800: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.GTEQ, "col1", "456");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 GTEQ 456: " + records.size());
//                ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.GTEQ, "col1", "600");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 GTEQ 600: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.IN, "col1", "400", "700");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 IN 400, 700: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.IN, "col1", "456", "789");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 IN 456 789: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.NOT_IN, "col1", "456", "789");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NOT_IN 456 789: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.NOT_IN, "col1", "400", "700");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NOT_IN 400 700: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.BETWEEN, "col1", "400", "700");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 BETWEEN 400, 700: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.BETWEEN, "col1", "456", "789");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 BETWEEN 456 789: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.NOT_BETWEEN, "col1", "456", "789");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NOT_BETWEEN 456 789: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//
//            pks = tbl.search(Operators.NOT_BETWEEN, "col1", "400", "700");
//            records = tbl.getAllRecords(pks);
//            if (records != null) {
//                System.out.println("num items col1 NOT_BETWEEN 400 700: " + records.size());
//                 ListIterator<Object> iter = records.listIterator();
//                while (iter.hasNext()) {
//                    System.out.println(iter.next().toString());
//                }
//            }
//        } catch (OperationException e) {
//            System.out.println("test failed: " + e.getMessage());
//        }
//    }
//
////    @Test(threadPoolSize = 3, invocationCount = 100, timeOut = 10000)
////    public void testCase6() {
////        System.out.println("testeCase6: " + tbl.getName());
////        try {
////            List<Object> records = tbl.search(Operators.LT, "col1", "500");
////            if (records != null) {
////                System.out.println("num items in col1 LT 500: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.GT, "col1", "500");
////            if (records != null) {
////                System.out.println("num items col1 GT 500: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.EQ, "col1", "456");
////            if (records != null) {
////                System.out.println("num items col1 EQ 456: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.NEQ, "col1", "123");
////            if (records != null) {
////                System.out.println("num items col1 NEQ 123: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.LTEQ, "col1", "789");
////            if (records != null) {
////                System.out.println("num items col1 LTEQ 789: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.LTEQ, "col1", "800");
////            if (records != null) {
////                System.out.println("num items col1 LTEQ 800: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.GTEQ, "col1", "456");
////            if (records != null) {
////                System.out.println("num items col1 GTEQ 456: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.GTEQ, "col1", "600");
////            if (records != null) {
////                System.out.println("num items col1 GTEQ 600: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.IN, "col1", "400", "700");
////            if (records != null) {
////                System.out.println("num items col1 IN 400, 700: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.IN, "col1", "456", "789");
////            if (records != null) {
////                System.out.println("num items col1 IN 456 789: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.NOT_IN, "col1", "456", "789");
////            if (records != null) {
////                System.out.println("num items col1 NOT_IN 456 789: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.NOT_IN, "col1", "400", "700");
////            if (records != null) {
////                System.out.println("num items col1 NOT_IN 400 700: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.BETWEEN, "col1", "400", "700");
////            if (records != null) {
////                System.out.println("num items col1 BETWEEN 400, 700: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.BETWEEN, "col1", "456", "789");
////            if (records != null) {
////                System.out.println("num items col1 BETWEEN 456 789: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.NOT_BETWEEN, "col1", "456", "789");
////            if (records != null) {
////                System.out.println("num items col1 NOT_BETWEEN 456 789: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////
////            records = tbl.search(Operators.NOT_BETWEEN, "col1", "400", "700");
////            if (records != null) {
////                System.out.println("num items col1 NOT_BETWEEN 400 700: " + records.size());
////                Iterator iter = records.iterator();
////                while (iter.hasNext()) {
////                    System.out.println(iter.next().toString());
////                }
////            }
////        } catch (OperationException e) {
////            System.out.println("test failed: " + e.getMessage());
////        }
//    //   }
}
