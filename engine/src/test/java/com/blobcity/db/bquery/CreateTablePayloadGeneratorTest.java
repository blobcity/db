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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.columntypes.NumberField;
import com.blobcity.db.lang.columntypes.StringField;
import com.blobcity.db.schema.IndexTypes;
import com.blobcity.db.schema.Types;
import com.blobcity.db.sql.CreateTablePayloadGenerator;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * Tests for Create Table payload generator. The test classes do not have support for column constraints and precision/
 * scale specification that is required with various data types as those are not currently supported by the SQL layer.
 *
 * @author akshaydewan
 */
public class CreateTablePayloadGeneratorTest {

    public CreateTablePayloadGeneratorTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * This is a PRIMARY_KEY test, whereas PK's are no longer allowed to be user defined. This test must be revised
     * or removed all together.
     */
//    @Test
//    public void test() throws OperationException, IOException {
//        CreateTablePayloadGenerator generator = new CreateTablePayloadGenerator();
//        generator.putColumn("col1", new StringField(Types.STRING));
//        generator.putColumn("col2", new NumberField(Types.FLOAT));
//        generator.putColumn("pkCol", new NumberField(Types.INTEGER));
//        generator.addUniqueConstraint("col1");
//        generator.setPrimaryKey("pkCol");
//
//        JSONObject pkColJSON = new JSONObject().put("type", new NumberField(Types.INTEGER).toJson());
//        JSONObject col1JSON = new JSONObject().put("type", new StringField(Types.STRING).toJson()).put("index", IndexTypes.UNIQUE.getText());
//        JSONObject col2JSON = new JSONObject().put("type", new NumberField(Types.FLOAT).toJson());
//        JSONObject metaJson = new JSONObject().put("table-type", "on-disk").put("replication-type", "distributed").put("replication-factor", 0).put("flexible-schema", false);
//        JSONObject columnsJson = new JSONObject().put("primary", "pkCol");
//        columnsJson.put("col1", col1JSON).put("col2", col2JSON).put("pkCol", pkColJSON);
//        JSONObject expectedPayload = new JSONObject().put("meta", metaJson).put("cols", columnsJson);
//
//        try {
//            JSONObject payload = generator.generate();
//            System.out.println("######: " + payload.toString());
//            JSONAssert.assertEquals(expectedPayload, payload, false);
//        } catch (OperationException ex) {
//            fail("Unexpected OperationException: " + ex.getMessage());
//        }
//    }

    @Test
    public void testWithoutPK() throws OperationException, IOException {
        CreateTablePayloadGenerator generator = new CreateTablePayloadGenerator();
        generator.putColumn("col1", new StringField(Types.STRING));
        generator.putColumn("col2", new NumberField(Types.FLOAT));
        generator.addUniqueConstraint("col1");
        try {
            generator.generate();
            fail("OperationExeption was expected");
        } catch (OperationException ex) {
            assertTrue(ex.getErrorCode().equals(ErrorCode.OPERATION_NOT_SUPPORTED));
        }

    }

}
