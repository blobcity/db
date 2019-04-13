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

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.statements.CreateTableExecutor;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import java.io.IOException;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;

/**
 * Yet another useless test class
 *
 * @author akshaydewan
 */
public class CreateTableExecutorTest {

//    final String appId = UUID.randomUUID().toString();
//    CreateTableExecutor createTableExecutor;
//
//    public CreateTableExecutorTest() {
//    }
//
//    @Before
//    public void setUp() {
//        createTableExecutor = new CreateTableExecutor();
//        //-----table manager mock
//        BSqlCollectionManager tableManagerMock = mock(BSqlCollectionManager.class);
//        createTableExecutor.setTableManager(tableManagerMock);
//    }
//
//    @After
//    public void tearDown() {
//    }
//
//    private String runQuery(String sql) throws OperationException, StandardException, IOException {
//        SQLParser parser = new SQLParser();
//        StatementNode stmt = parser.parseStatement(sql);
//        return createTableExecutor.execute(appId, stmt);
//    }
//
//    @Test
//    public void simpleTest() throws OperationException, StandardException, IOException, JSONException {
//        String response = runQuery("CREATE TABLE films (\n"
//                + "    code        char(5) CONSTRAINT firstkey PRIMARY KEY,\n"
//                + "    title       varchar(40) NOT NULL,\n"
//                + "    did         integer NOT NULL,\n"
//                + "    date_prod   varchar(20),\n"
//                + "    kind        varchar(10),\n"
//                + "    len         bigint\n"
//                + ")");
//        JSONObject responseJSON = new JSONObject(response);
//        assertTrue(responseJSON.getInt("ack") == 1);
//    }
//
//    @Test
//    public void unsupportedDataTypeTest() throws StandardException, IOException {
//        try {
//            runQuery("CREATE TABLE films (\n"
//                    + "    code        char(5) CONSTRAINT firstkey PRIMARY KEY,\n"
//                    + "    title       varchar(40) NOT NULL,\n"
//                    + "    did         integer NOT NULL,\n"
//                    + "    date_prod   varchar(20),\n"
//                    + "    kind        varchar(10),\n"
//                    + "    len         interval hour to minute\n"
//                    + ")");
//            fail("OperationException was not thrown");
//        } catch (OperationException ex) {
//            assertTrue(ex.getErrorCode().equals(ErrorCode.OPERATION_NOT_SUPPORTED));
//        }
//    }
//
//    @Test
//    public void uniqueKeyTest() throws OperationException, StandardException, IOException, JSONException {
//        String response = runQuery("CREATE TABLE films (\n"
//                + "    code        char(5) PRIMARY KEY,\n"
//                + "    title       varchar(40),\n"
//                + "    did         integer,\n"
//                + "    date_prod   integer,\n"
//                + "    kind        varchar(10),\n"
//                + "    len         bigint,\n"
//                + "    CONSTRAINT production UNIQUE(date_prod)\n"
//                + ")");
//        JSONObject responseJSON = new JSONObject(response);
//        assertTrue(responseJSON.getInt("ack") == 1);
//    }
//
//    @Test
//    public void collateTest() throws OperationException, StandardException, IOException, JSONException {
//        String response = runQuery("CREATE TABLE films (\n"
//                + "    code        char(5) PRIMARY KEY,\n"
//                + "    title       varchar(40),\n"
//                + "    did         integer,\n"
//                + "    date_prod   integer,\n"
//                + "    kind        varchar(10) COLLATE donkeykong,\n"
//                + "    len         bigint,\n"
//                + "    CONSTRAINT production UNIQUE(date_prod)\n"
//                + ")");
//        JSONObject responseJSON = new JSONObject(response);
//        assertTrue(responseJSON.getInt("ack") == 1);
//        JSONArray warnings = responseJSON.getJSONArray("warnings");
//        assertTrue(warnings.length() >= 1);
//    }

}
