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
import com.blobcity.db.sql.statements.DropTableExecutor;
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
import org.junit.Test;
import static org.mockito.Mockito.mock;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 *
 * @author akshaydewan
 */
public class DropTableExecutorTest {

    final String appId = UUID.randomUUID().toString();
    DropTableExecutor dropTableExecutor;

    @Before
    public void setUp() {
        dropTableExecutor = new DropTableExecutor();
        //-----table manager mock
        BSqlCollectionManager tableManagerMock = mock(BSqlCollectionManager.class);
        dropTableExecutor.setTableManager(tableManagerMock);
    }

    @After
    public void tearDown() {
    }

    private void assertAck(final String response) throws JSONException {
        JSONObject responseJSON = new JSONObject(response);
        JSONAssert.assertEquals(JsonMessages.SUCCESS_ACKNOWLEDGEMENT_OBJECT, responseJSON, false);
    }

    private void checkSupported(final String sql) throws OperationException, StandardException, JSONException {
        SQLParser parser = new SQLParser();
        StatementNode stmt = parser.parseStatement(sql);
        String response = dropTableExecutor.execute(appId, stmt);
        assertAck(response);
    }

    private void checkUnsupported(final String sql) throws StandardException {
        SQLParser parser = new SQLParser();
        StatementNode stmt = parser.parseStatement(sql);
        try {
            dropTableExecutor.execute(appId, stmt);
            fail("OperationException was expected");
        } catch (OperationException ex) {
            assertTrue(ex.getErrorCode().equals(ErrorCode.OPERATION_NOT_SUPPORTED));
        }
    }

    @Test
    public void test() throws StandardException, OperationException, JSONException {
        checkSupported("DROP TABLE sometable");
    }

    @Test
    public void testCascade() throws StandardException {
        checkUnsupported("DROP TABLE sometable CASCADE");
    }

    @Test
    public void testRestrict() throws StandardException {
        checkUnsupported("DROP TABLE sometable RESTRICT");
    }
}
