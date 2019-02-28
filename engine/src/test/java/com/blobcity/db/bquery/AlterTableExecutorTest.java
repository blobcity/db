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

import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.statements.AlterTableExecutor;
import com.blobcity.util.json.JsonMessages;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 *
 * @author akshaydewan
 */
public class AlterTableExecutorTest {

//    final String appId = UUID.randomUUID().toString();
//    AlterTableExecutor alterTableExecutor;
//
//    @Before
//    public void setUp() {
//        alterTableExecutor = new AlterTableExecutor();
//        //-----table manager mock
//        BSqlCollectionManager tableManagerMock = mock(BSqlCollectionManager.class);
//        alterTableExecutor.setTableManager(tableManagerMock);
//        BSqlIndexManager indexManagerMock = mock(BSqlIndexManager.class);
//        alterTableExecutor.setIndexManager(indexManagerMock);
//    }
//
//    @After
//    public void tearDown() {
//    }
//
//    private void assertAck(final String response) throws JSONException {
//        JSONObject responseJSON = new JSONObject(response);
//        JSONAssert.assertEquals(JsonMessages.SUCCESS_ACKNOWLEDGEMENT_OBJECT, responseJSON, false);
//    }
//
//    private void checkSupported(final String sql) throws OperationException, StandardException, JSONException {
//        SQLParser parser = new SQLParser();
//        StatementNode stmt = parser.parseStatement(sql);
//        String response = alterTableExecutor.execute(appId, stmt);
//        assertAck(response);
//    }
//
//    private void checkUnsupported(final String sql) throws StandardException {
//        SQLParser parser = new SQLParser();
//        StatementNode stmt = parser.parseStatement(sql);
//        try {
//            alterTableExecutor.execute(appId, stmt);
//            fail("OperationException was expected");
//        } catch (OperationException ex) {
//            assertTrue(ex.getErrorCode().equals(ErrorCode.OPERATION_NOT_SUPPORTED));
//        }
//    }
//
////    @Test
////    public void test() throws StandardException {
////        SQLParser parser = new SQLParser();
////        String[] sql = new String[]{"ALTER TABLE distributors \n"
////            + "DROP COLUMN address CASCADE\n",
////            "ALTER TABLE distributors \n"
////            + "DROP COLUMN address RESTRICT\n",
////            "ALTER TABLE distributors ADD COLUMN address varchar(30)",
////            "ALTER TABLE distributors ALTER COLUMN address DROP DEFAULT",
////            "ALTER TABLE distributors ALTER COLUMN address SET DEFAULT 'abc'",
////            "ALTER TABLE distributors ALTER COLUMN id RESTART WITH 100",
////            "ALTER TABLE distributors ADD UNIQUE (somecol)",
////            "ALTER TABLE distributors ADD PRIMARY KEY (somecol)",
////            "ALTER TABLE distributors ALTER COLUMN address NOT NULL",};
////        for (String s : sql) {
////            try {
////                StatementNode stmt = parser.parseStatement(s);
////                alterTableExecutor.execute(appId, stmt);
////            } catch (OperationException ex) {
////                LoggerFactory.getLogger(AlterTableExecutorTest.class.getName()).log(Level.INFO, ex.getErrorCode().getErrorMessage(), ex);
////            }
////        }
////    }
//    @Test
//    public void dropColumn() throws StandardException, OperationException, JSONException {
//        checkSupported("ALTER TABLE distributors DROP COLUMN address");
//        checkSupported("ALTER TABLE distributors DROP address");
//    }
//
//    @Test
//    public void dropColumnCascade() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors DROP COLUMN address CASCADE");
//        checkUnsupported("ALTER TABLE distributors DROP address CASCADE");
//    }
//
//    @Test
//    public void dropColumnRestrict() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors DROP COLUMN address RESTRICT");
//        checkUnsupported("ALTER TABLE distributors DROP address RESTRICT");
//    }
//
//    @Test
//    public void addColumn() throws StandardException, OperationException, JSONException {
//        checkSupported("ALTER TABLE distributors ADD COLUMN address varchar(30)");
//    }
//
//    @Test
//    public void dropDefault() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors ALTER COLUMN address DROP DEFAULT");
//    }
//
//    @Test
//    public void setDefault() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors ALTER COLUMN address SET DEFAULT 'abc'");
//    }
//
//    @Test
//    public void restartIncrement() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors ALTER COLUMN id RESTART WITH 100");
//    }
//
//    @Test
//    public void addUnique() throws StandardException, OperationException, JSONException {
//        checkSupported("ALTER TABLE distributors ADD UNIQUE (somecol)");
//    }
//
//    @Test
//    public void addMutipleUnique() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors ADD UNIQUE (col1,col2)");
//    }
//
//    @Test
//    public void addPrimaryKey() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors ADD PRIMARY KEY (somecol)");
//    }
//
//    @Test
//    public void makeNotNull() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors ALTER COLUMN address NOT NULL");
//    }
//
//    @Test
//    public void addForeignKey() throws StandardException {
//        checkUnsupported("ALTER TABLE distributors ADD FOREIGN KEY (UserId) REFERENCES Users(Id)");
//    }
//
//    @Test
//    @Ignore
//    public void addForeignKeyColumn() throws StandardException {
//        //Parser does not support this
//        checkUnsupported("ALTER TABLE distributors ADD COLUMN UserId int(11) REFERENCES Users(Id)");
//    }
//
//    @Test
//    @Ignore
//    public void addScope() throws StandardException {
//        //Parser does not support this
//        checkUnsupported("ALTER TABLE distributors ALTER COLUMN address ADD SCOPE Foo");
//    }
//
//    @Test
//    @Ignore
//    public void dropScopeCascade() throws StandardException {
//        //Parser does not support this
//        checkUnsupported("ALTER TABLE distributors ALTER COLUMN address DROP SCOPE CASCADE");
//    }
//
//    @Test
//    @Ignore
//    public void dropScopeRestrict() throws StandardException {
//        //Parser does not support this
//        checkUnsupported("ALTER TABLE distributors ALTER COLUMN address DROP SCOPE RESTRICT");
//    }
//
//    @Test
//    public void collateTest() throws StandardException, OperationException, JSONException {
//        SQLParser parser = new SQLParser();
//        StatementNode stmt = parser.parseStatement("ALTER TABLE distributors ADD COLUMN address varchar(30) COLLATE moocowlang");
//        String response = alterTableExecutor.execute(appId, stmt);
//        JSONObject responseJSON = new JSONObject(response);
//        assertTrue(responseJSON.getInt("ack") == 1);
//        assertTrue(responseJSON.getJSONArray("warnings").length() >= 1);
//    }

}
