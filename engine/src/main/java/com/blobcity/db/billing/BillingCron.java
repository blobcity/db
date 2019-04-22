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

import com.blobcity.db.bquery.SQLExecutorBean;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryStore;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sanketsarang
 */
@Component
public class BillingCron {

    private static final Logger logger = LoggerFactory.getLogger(BillingCron.class.getName());

    @Autowired
    private SQLExecutorBean sqlExecutor;
    @Autowired @Lazy
    private BSqlDataManager dataManager;

    @Scheduled(cron = "0 * * * * *")
    public synchronized void computeBilling() {
        final JSONObject jsonObject = new JSONObject(sqlExecutor.executePrivileged(".systemdb", "select * from `.systemdb`.`SelectActivityLog`"));
        final JSONArray dataArray = jsonObject.getJSONArray("p");

        final Map<String, Long> selectCountMap = new HashMap<>();
        for(int i=0; i < dataArray.length(); i++) {
            JSONObject selectLog = dataArray.getJSONObject(i);
            final String ds = selectLog.getString("ds");
            if(!selectCountMap.containsKey(ds)) {
                selectCountMap.put(ds, 0L);
            }

            selectCountMap.put(ds, selectCountMap.get(ds) + selectLog.getLong("rows"));

            /* Delete the entry from SelectActivityLog */
            try {
                dataManager.remove(".systemdb", "SelectActivityLog", selectLog.getString("_id"));
            } catch (OperationException e) {
                logger.error("Cannot update BillingUsage until accounted SelectActivityLog entry is deleted", e);
                return;
            }
        }

        DateFormat df = new SimpleDateFormat("MM-yy");
        final String my = df.format(new Date(System.currentTimeMillis())); //month and year

        selectCountMap.forEach((ds, increment) -> {
            final JSONObject billingJsonResponse = new JSONObject(
                    sqlExecutor.executePrivileged(".systemdb", "select * from `.systemdb`.`BillingUsage` where " +
                            "`ds` = '" + ds + "' and `my` = '" + my + "'"));

            if(billingJsonResponse.getJSONArray("p").length() == 0) {
                JSONObject insertJson = new JSONObject();
                insertJson.put("ds", ds);
                insertJson.put("my", my);
                insertJson.put("rows", increment);
                try {
                    dataManager.insert(".systemdb", "BillingUsage", insertJson);
                } catch (OperationException e) {
                    logger.error("Unable to insert new billing activity into BillingUsage table. Customer will be under billed", e);
                }
            } else {
                JSONObject currentBillingUsage = billingJsonResponse.getJSONArray("p").getJSONObject(0);
                final long newRows = currentBillingUsage.getLong("rows") + increment;

                sqlExecutor.executePrivileged(".systemdb", "update `.systemdb`.`BillingUsage` set `rows` = " + newRows
                        + " where `ds` = '" + ds + "' and `my` = '" + my + "'");
            }
        });
    }
}
