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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.json.JSONArray;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link CollectionField}
 *
 * @author sanketsarang
 */
public class CollectionFieldTest {

    /**
     * Test of getQueryCode method, of class CollectionField.
     */
    @Test
    public void testGetType() throws OperationException {
        System.out.println("getQueryCode");
        CollectionField instance;

        instance = new CollectionField(Types.ARRAY, Types.STRING);
        assertEquals(Types.ARRAY, instance.getType());

        instance = new CollectionField(Types.MULTISET, Types.STRING);
        assertEquals(Types.MULTISET, instance.getType());
    }

    /**
     * Test of getSubType method, of class CollectionField.
     */
    @Test
    public void testGetSubType() throws OperationException {
        System.out.println("getSubType");
        CollectionField instance;

        for (Types type : Types.values()) {
            instance = new CollectionField(Types.ARRAY, type);
            assertEquals(type, instance.getSubType());
        }

        for (Types type : Types.values()) {
            instance = new CollectionField(Types.MULTISET, type);
            assertEquals(type, instance.getSubType());
        }
    }

    /**
     * Test of getDefaultValue method, of class CollectionField.
     */
    @Test
    public void testGetDefaultValue() throws OperationException {
        System.out.println("getDefaultValue");
        CollectionField instance = null;

        instance = new CollectionField(Types.ARRAY, Types.STRING);
        assertNull(instance.getDefaultValue());

        instance = new CollectionField(Types.MULTISET, Types.STRING);
        assertNull(instance.getDefaultValue());
    }

    /**
     * Test of hasDefaultValue method, of class CollectionField.
     */
    @Test
    public void testHasDefaultValue() throws OperationException {
        System.out.println("hasDefaultValue");
        CollectionField instance;

        instance = new CollectionField(Types.ARRAY, Types.STRING);
        assertFalse(instance.hasDefaultValue());

        instance = new CollectionField(Types.MULTISET, Types.STRING);
        assertFalse(instance.hasDefaultValue());
    }

    /**
     * Test of convert method, of class CollectionField.
     */
    @Test
    public void testConvert() throws Exception {
        System.out.println("convert");
        CollectionField instance;

        /* Test with Lists */
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        
        instance = new CollectionField(Types.ARRAY, Types.INT);
        Object convertedObj = instance.convert(list);
        if(!(convertedObj instanceof List)) {
            fail("convert method giving a non List object for an ARRAY collection");
        }
        
        instance = new CollectionField(Types.MULTISET, Types.INT);
        convertedObj = instance.convert(list);
        if(!(convertedObj instanceof Set)) {
            fail("convert method giving a non Set object for an MULTISET collection. Got: " + convertedObj.getClass().getName());
        }
        
        /* Test with JSONArray */
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < 10; i++) {
            jsonArray.put(i);
        }
        
        instance = new CollectionField(Types.ARRAY, Types.INT);
        convertedObj = instance.convert(jsonArray);
        if(!(convertedObj instanceof List)) {
            fail("convert method giving a non List object for an ARRAY collection");
        }
        
        instance = new CollectionField(Types.MULTISET, Types.INT);
        convertedObj = instance.convert(jsonArray);
        if(!(convertedObj instanceof Set)) {
            fail("convert method giving a non Set object for an MULTISET collection");
        }
        
        try {
            instance.convert(null);
            fail("null value not checked");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
    }
}
