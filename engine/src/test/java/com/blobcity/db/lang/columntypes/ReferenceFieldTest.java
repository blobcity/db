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

package com.blobcity.db.lang.columntypes;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Types;
import java.util.UUID;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link ReferenceField}
 *
 * @author sanketsarang
 */
public class ReferenceFieldTest {

    /**
     * Test of getQueryCode method, of class ReferenceField.
     */
    @Test
    public void testGetType() {
        System.out.println("getQueryCode");
        assertEquals(Types.REF, new ReferenceField().getType());
        assertEquals(Types.REF, new ReferenceField("table").getType());
    }

    /**
     * Test of getScope method, of class ReferenceField.
     */
    @Test
    public void testGetScope() {
        System.out.println("getScope");

        assertNull(new ReferenceField().getScope());
        assertEquals("table", new ReferenceField("table").getScope());
    }

    /**
     * Test of isScoped method, of class ReferenceField.
     */
    @Test
    public void testIsScoped() {
        System.out.println("isScoped");

        assertFalse(new ReferenceField().isScoped());
        assertTrue(new ReferenceField("table").isScoped());
    }

    /**
     * Test of getDefaultValue method, of class ReferenceField.
     */
    @Test
    public void testGetDefaultValue() {
        System.out.println("getDefaultValue");

        assertNull(new ReferenceField().getDefaultValue());
        assertNull(new ReferenceField("table").getDefaultValue());
    }

    /**
     * Test of hasDefaultValue method, of class ReferenceField.
     */
    @Test
    public void testHasDefaultValue() {
        System.out.println("hasDefaultValue");

        assertFalse(new ReferenceField().hasDefaultValue());
        assertFalse(new ReferenceField("table").hasDefaultValue());
    }

    /**
     * Test of convert method, of class ReferenceField.
     */
    @Test
    public void testConvert() throws Exception {
        System.out.println("convert");
        String refValue = UUID.randomUUID().toString() + ":" + UUID.randomUUID().toString();
        ReferenceField instance = new ReferenceField();

        assertEquals(refValue, instance.convert(refValue));
        try {
            instance.convert(null);
            fail("null value not checked");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
        try {
            instance.convert("");
            fail("invalid REF formated accepted");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.INVALID_REF_FORMAT, ex.getErrorCode());
        }
        try {
            instance.convert(UUID.randomUUID().toString());
            fail("invalid REF formated accepted");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.INVALID_REF_FORMAT, ex.getErrorCode());
        }
        try {
            instance.convert(1000);
            fail("invalid input data type accepted");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
    }

}
