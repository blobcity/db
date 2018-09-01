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
import org.json.JSONObject;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link BinaryField}
 *
 * @author sanketsarang
 */
public class BooleanFieldTest {

    /**
     * Test of getQueryCode method, of class BooleanField.
     */
    @Test
    public void testGetType() {
        System.out.println("getQueryCode");
        BooleanField instance = new BooleanField();
        Types expResult = Types.BOOLEAN;
        Types result = instance.getType();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDefaultValue method, of class BooleanField.
     */
    @Test
    public void testGetDefaultValue() {
        System.out.println("getDefaultValue");
        BooleanField instance = new BooleanField();
        assertEquals(null, instance.getDefaultValue());

        instance = new BooleanField(true);
        assertTrue(instance.getDefaultValue());

        instance = new BooleanField(false);
        assertFalse(instance.getDefaultValue());
    }

    /**
     * Test of hasDefaultValue method, of class BooleanField.
     */
    @Test
    public void testHasDefaultValue() {
        System.out.println("hasDefaultValue");
        BooleanField instance = new BooleanField();
        assertFalse(instance.hasDefaultValue());
        
        instance = new BooleanField(true);
        assertTrue(instance.hasDefaultValue());
        
        instance = new BooleanField(false);
        assertTrue(instance.hasDefaultValue());
    }

    /**
     * Test of convert method, of class BooleanField.
     */
    @Test
    public void testConvert() throws Exception {
        System.out.println("convert");
        BooleanField instance = new BooleanField();
        assertTrue(instance.convert("true"));
        assertFalse(instance.convert("false"));
        assertTrue(instance.convert(true));
        assertFalse(instance.convert(false));
        
        /* Test with getting value from JSON as it is the expected source for data*/
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("true-key", true);
        jsonObject.put("false-key", false);
        assertTrue(instance.convert(jsonObject.get("true-key")));
        assertFalse(instance.convert(jsonObject.get("false-key")));
        
        try {
            instance.convert(null);
            fail("null incorrectly accepted as a valid input");
        } catch(OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
        
        try {
            instance.convert("something");
            fail("incorrectly accepted a non boolean string as a value boolean value");
        } catch(OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
        
        try {
            instance.convert(new Integer(0));
            fail("incorrectly accepted an Integer as a valid Boolean value");
        } catch(OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
    }
}
