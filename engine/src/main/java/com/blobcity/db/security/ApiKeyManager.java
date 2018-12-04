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

package com.blobcity.db.security;

import com.blobcity.db.bquery.SQLExecutorBean;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.util.json.JsonUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Provides functions to create new API keys, drop keys and validate keys
 *
 * @author sanketsarang
 */
@Component
public class ApiKeyManager {

    @Autowired
    private BSqlDataManager dataManager;
    @Autowired @Lazy
    private SQLExecutorBean sqlExecutor;

    public String createDsKey(final String ds) throws OperationException {
        final String newKey = keyGen("DS");
        JSONObject record = new JSONObject();
        record.put("key", newKey);
        record.put("ds", ds);
        dataManager.insert(BSql.SYSTEM_DB, "ApiKeys", record);
        return newKey;
    }

    public String createMasterKey() throws OperationException {
        final String newKey = keyGen("BC");
        JSONObject record = new JSONObject();
        record.put("key", newKey);
        dataManager.insert(BSql.SYSTEM_DB, "ApiKeys", record);
        return newKey;
    }

    public void validateKey(final String key) throws OperationException {
        String sql = "SELECT * FROM `.systemdb`.`ApiKeys` WHERE `key`='" + key + "'";
        JSONObject responseJson = new JSONObject(sqlExecutor.executePrivileged(".systemdb", sql));
        if(!JsonUtil.isAck(responseJson)) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error occurred in validation API access key");
        }

        if(responseJson.getJSONArray("p").length() != 1) {
            throw new OperationException(ErrorCode.INVALID_API_KEY);
        }
    }

    public void validateDsAccess(final String key, final String ds) throws OperationException {
        String sql = "SELECT * FROM `.systemdb`.`ApiKeys` WHERE `key`='" + key + "'";
        JSONObject responseJson = new JSONObject(sqlExecutor.executePrivileged(".systemdb", sql));
        if(!JsonUtil.isAck(responseJson)) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error occurred in validation API access key");
        }

        if(responseJson.getJSONArray("p").length() != 1) {
            throw new OperationException(ErrorCode.INVALID_API_KEY);
        }

        JSONObject jsonObject = responseJson.getJSONArray("p").getJSONObject(0);

        if(!jsonObject.getString("ds").equals(ds)) {
            throw new OperationException(ErrorCode.INVALID_API_KEY);
        }
    }

    public void revokeKey(final String key) {
        String sql = "DELETE FROM `.systemdb`.`ApiKeys` WHERE `key` = '" + key + "'";
        sqlExecutor.executePrivileged(".systemdb", sql);
    }

    public List<String> listKeys() throws OperationException {
        return fetchKeys("SELECT * FROM `.systemdb`.`ApiKeys`");
    }

    public List<String> listDsKeys(final String ds) throws OperationException {
        return fetchKeys("SELECT * FROM `.systemdb`.`ApiKeys` where `ds` = '" + ds + "'");
    }

    private String keyGen(final String prefix) {
        return prefix + "." + UUID.randomUUID().toString().replaceAll("-","") + UUID.randomUUID().toString().replaceAll("-","");
    }

    private List<String> fetchKeys(final String sql) throws OperationException {
        JSONObject responseJson = new JSONObject(sqlExecutor.executePrivileged(".systemdb", sql));
        if(!JsonUtil.isAck(responseJson)) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error occurred in validation API access key");
        }

        final JSONArray jsonArray = responseJson.getJSONArray("p");
        final List<String> keys = new ArrayList<>();

        for(int i = 0; i < jsonArray.length(); i++) {
            keys.add(jsonArray.getJSONObject(i).getString("key"));
        }

        return keys;
    }
}
