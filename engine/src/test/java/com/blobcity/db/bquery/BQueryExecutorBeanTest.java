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

package com.blobcity.db.bquery;

import com.blobcity.db.constants.BQueryCommands;
import java.util.UUID;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author sanketsarang
 */
@Ignore //TODO make test cases self-contained
public class BQueryExecutorBeanTest {

    private JSONObject baseJson;
    @Autowired
    private BQueryExecutorBean bqueryExecutor;

    public BQueryExecutorBeanTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        try {
            baseJson = new JSONObject();
            baseJson.put("appId", "test");
            baseJson.put("appKey", "test");
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of runQuery method, of class BQueryExecutorBean.
     */
    @Test
    public void testRunQueryCreateTable() throws Exception {
        System.out.println("Need to re-implement this test");
//        System.out.println("runQuery: create-table");
//        JSONObject jsonObject = new JSONObject(baseJson);
//        final String tableName = UUID.randomUUID().toString();
//        jsonObject.put("t", tableName);
//        jsonObject.put("q", BQueryCommands.CREATE_TABLE.getCommand());
//        String expResult = "{\"ack\":\"1\"}";
//        System.out.println("Firing query: " + jsonObject.toString());
//        String result = bqueryExecutor.runQuery(jsonObject.toString());
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("create-table command failed");
    }
}
