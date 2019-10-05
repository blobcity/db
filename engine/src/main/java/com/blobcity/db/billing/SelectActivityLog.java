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

package com.blobcity.db.billing;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.features.FeatureRules;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

/**
 * @author sanketsarang
 */
@Component
@Configuration
@EnableAsync
public class SelectActivityLog {

    private static final Logger logger = LoggerFactory.getLogger(SelectActivityLog.class.getName());

    @Autowired
    private BSqlDataManager dataManager;

    /**
     * Used for query performance analysis. Stores execution information related to the query
     * @param ds name of datastore
     * @param collection name of collection
     * @param sql the actual SQL query
     * @param rows number of rows returned
     * @param time the execution time in milli-seconds
     */
    @Async
    public void registerSelectQuery(final String ds, final String collection, final String sql, final long rows, final long time) {
        if(!FeatureRules.QPS) return;
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ds", ds);
        jsonObject.put("c", collection);
        jsonObject.put("sql", sql);
        jsonObject.put("rows", rows);
        jsonObject.put("time", time);
        jsonObject.put("timestamp", System.currentTimeMillis());
        try{
            dataManager.insert(".systemdb", "QPS", jsonObject);
        } catch(OperationException ex) {
            ex.printStackTrace();
        }
    }

    @Async
    public void registerActivity(final String ds, final long rows) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ds", ds);
        jsonObject.put("rows", rows);
        try {
            dataManager.insert(".systemdb", "SelectActivityLog", jsonObject);
        } catch (OperationException e) {
            e.printStackTrace();
        }
    }
}
