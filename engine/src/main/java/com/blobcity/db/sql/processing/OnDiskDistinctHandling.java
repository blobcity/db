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

package com.blobcity.db.sql.processing;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author sanketsarang
 */
@Component
public class OnDiskDistinctHandling {

    public void keepDistinct(final String ds, final String collection, final Map<String, List<JSONObject>> resultMap, List<String> distinctColumns) throws OperationException {

    }

    /**
     * Call when SQL query requests only SELECT DISTINCT without a WHERE, GROUP BY, HAVING and AGGREGATE operations
     * @param ds name of datastore
     * @param collection name of collection
     * @param columns columns to use for distinct. Must not includ SELECT DISTINCT *
     * @throws OperationException if an error occurs in processing the operation
     * @returns records that are distinct across the requested columns
     */
    public List<JSONObject> justDistinct(final String ds, final String collection, final List<String> columns) throws OperationException {
        if(columns.size() == 1) {
            /* Use column cardinality for this operation */
        } else {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "select distinct with multiple columns not yet optimised");
        }

        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
}
