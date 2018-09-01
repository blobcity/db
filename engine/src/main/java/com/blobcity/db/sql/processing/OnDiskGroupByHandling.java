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

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.statements.SelectExecutor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Offers utility methods to handle GROUP BY processing for OnDisk queries
 *
 * @author sanketsarang
 */
@Component
public class OnDiskGroupByHandling {
    private static final Logger logger = LoggerFactory.getLogger(OnDiskGroupByHandling.class.getName());

    @Autowired @Lazy
    private BSqlDataManager dataManager;
    @Autowired @Lazy
    private BSqlIndexManager indexManager;


    public Map<String, List<JSONObject>> fullGroupedRecords(final String ds, final String collection, final List<String> columnNames) throws OperationException {
        Map<String, List<String>> groupedKeys = fullGroupedKeys(ds, collection, columnNames);
        Map<String, List<JSONObject>> groupedMap = new HashMap<>();

        groupedKeys.forEach((mapKey, value) -> {
            try {
                List<JSONObject> list = new ArrayList<>();
                list.add(dataManager.select(ds, collection, value.get(0)));
                groupedMap.put(mapKey, list);
            } catch (OperationException e) {
                logger.error(e.getMessage(), e);
            }
        });

        return groupedMap;
    }

    public Map<String, List<String>> fullGroupedKeys(final String ds, final String collection, final List<String> columnNames) throws OperationException {
        Map<String, List<String>> groupedKeys = new HashMap<>();

        Map<String, Set<String>> columnCardinalsMaps = new HashMap<>();
        columnNames.parallelStream().forEach(columName -> {
            try {
                Set<String> cardinalsSet = new HashSet<>();
                Iterator<String> iterator = indexManager.getCardinals(ds, collection, columName);
                iterator.forEachRemaining(cardinal -> cardinalsSet.add(cardinal));
                columnCardinalsMaps.put(columName, cardinalsSet);
            } catch (OperationException e) {
                logger.error(e.getMessage(), e);
            }
        });

        if(columnCardinalsMaps.size() != columnNames.size()) {
            /* Indicates OperationException was thrown in previous loop */
            throw new OperationException(ErrorCode.GROUP_BY);
        }

        if(columnCardinalsMaps.size() == 1) {
            /* Short circuit if group by on single column */
            columnCardinalsMaps.forEach((columnName, columnValue) -> {
                JSONArray array = new JSONArray();
                array.put(columnValue);
                try {
                    List<String> keys = new ArrayList<>();
                    keys.add(indexManager.getAnyCardinalEntry(ds, collection, columnName, columnValue));
                    groupedKeys.put(array.toString(), keys);
                } catch (OperationException e) {
                    logger.error(e.getMessage(), e);
                }
            });

            return groupedKeys;
        } else {
            /* Execute multiple columnes GROUP BY */

            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "GROUP BY on multiple columns currently not supported");
        }
    }

    public Map<String, List<JSONObject>> fullGroupRecordsWithFilter(final String ds, final String collection, final List<String> columnNames, final List<String> keys) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    public Map<String, List<JSONObject>> fullGroupKeysWithFilter(final String ds, final String collection, final List<String> columnNames, final List<String> keys) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
}
