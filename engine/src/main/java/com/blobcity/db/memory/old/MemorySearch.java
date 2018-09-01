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

package com.blobcity.db.memory.old;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.Operators;
import com.blobcity.db.olap.DataCube;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.PostConstruct;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This is the prelim version of search in in-memory tables.
 * This implements a full table scan(Stupid, I know, but don't worry we are working on this).
 * This was just a test
 * 
 * @author sanketsarang
 */
@Component
public class MemorySearch {

    Logger logger = LoggerFactory.getLogger(MemorySearch.class);

    /**
     *
     */
    @PostConstruct
    public void init() {

    }

    /**
     *
     * @param app
     * @param table
     * @param column
     * @param value
     * @param op
     * @return
     * @throws OperationException
     */
    public synchronized Iterator<String> obsoleteSearch(String app, String table, String column, String value, Operators op) throws OperationException {
        try {
            //column = column.toLowerCase();
            String tableName = app + "." + table;
            List<String> items = new ArrayList<>();
            Iterator<Object> iter = MemoryTableStore.getTable(tableName).getAllKeys().iterator();
            DataCube cube = MemoryTableStore.getTable(tableName).getDataCube();

            JSONObject tmp;
            String currKey;
            while (iter.hasNext()) {
                currKey = iter.next().toString();
                tmp = MemoryTableStore.getTable(tableName).getData().getRecordAsJson(currKey.toString());
                switch (op) {
                    case EQ:
                        if (tmp.get(column).equals(value)) {
                            items.add(currKey);
                        }
                        break;
                    case NEQ:
                        if (!tmp.get(column).equals(value)) {
                            items.add(currKey);
                        }
                        break;
                    case GT:
                        if (tmp.getDouble(column) > Double.valueOf(value.toString())) {
                            items.add(currKey);
                        }
                        break;
                    case GTEQ:
                        if (tmp.getDouble(column) >= Double.valueOf(value.toString())) {
                            items.add(currKey);
                        }
                        break;
                    case LT:
                        if (tmp.getDouble(column) < Double.valueOf(value.toString())) {
                            items.add(currKey);
                        }
                        break;
                    case LTEQ:
                        if (tmp.getDouble(column) <= Double.valueOf(value.toString())) {
                            items.add(currKey);
                        }
                        break;
                    default:
                        break;
                    //do nothing
                }
            }
            return items.iterator();
        } catch (NullPointerException e) {
            logger.error("search failed: " + e.getMessage());
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not search memory table");
        }
    }

    /**
     *
     * @param op
     * @param app
     * @param table
     * @param colsToSelect
     * @param column
     * @param value
     * @return
     * @throws OperationException
     */
    public synchronized List<Object> search(Operators op, String app, String table, List<String> colsToSelect, String column, String... value) throws OperationException {
        //column = column.toLowerCase();
        String tableName = app + "." + table;
        DataCube cube = MemoryTableStore.getTable(tableName).getDataCube();
        List<Object> result = cube.searchDim(op, colsToSelect, column, (Object[]) value);
        return result;
    }
}
