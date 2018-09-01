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
import com.blobcity.db.requests.RequestHandlingBean;
import com.blobcity.lib.query.CollectionStorageType;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.RecordType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author sanketsarang
 */
@Component
public class InternalQueryBean {

    @Autowired
    private RequestHandlingBean requestHandlingBean;
    @Autowired
    private SQLExecutorBean sqlExecutorBean;

    public void insertJSON(final String datastore, final String collection, JSONObject jsonRow) {
        Query query = new Query();
        query.insertQueryUninferred(datastore, collection, Arrays.asList(new JSONObject[]{jsonRow}), RecordType.JSON);
        requestHandlingBean.newRequest(query);
    }

    public void insertJSON(final String datastore, final String collection, List<Object> jsonRows) {
        Query query = new Query();
        query.insertQueryUninferred(datastore, collection, jsonRows, RecordType.JSON);
        requestHandlingBean.newRequest(query);
    }

    public void insert(final String datastore, final String collection, Object record) {
        Query query = new Query();
        query.insertQueryUninferred(datastore, collection, Arrays.asList(new Object[]{record}), RecordType.AUTO);
        requestHandlingBean.newRequest(query);
    }

    public void insert(final String datastore, final String collection, List<Object> records) {
        Query query = new Query();
        query.insertQueryUninferred(datastore, collection, records, RecordType.AUTO);
        requestHandlingBean.newRequest(query);
    }

    public List<JSONObject> select(final String datastore, final String sql) throws OperationException {
        String responseString = sqlExecutorBean.executePrivileged(datastore, sql);
        JSONObject responseJson = new JSONObject(responseString);

        if(responseJson.get("ack").equals("0")) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        JSONArray responseArray = responseJson.getJSONArray("p");

        List<JSONObject> result = new ArrayList<>();
        for(int i = 0; i < responseArray.length(); i++) {
            result.add(responseArray.getJSONObject(i));
        }

        return result;
    }

    public void update(final String datastore, final String sql) {
       sqlExecutorBean.executePrivileged(datastore, sql);
    }

    public void delete(final String datastore, final String sql) {
        sqlExecutorBean.executePrivileged(datastore, sql);
    }

    public void createCollection(final String ds, final String collection, final CollectionStorageType collectionStorageType) {
        requestHandlingBean.newRequest(new Query().createCollection(ds, collection, collectionStorageType));
    }

    public boolean collectionExists(final String ds, final String collection) {
        Query result = requestHandlingBean.newRequest(new Query().listCollections(ds));

        if(result.isAckSuccess()) {
            List<Object> list = new ArrayList<>((Collection)result.getPayload());
            if(list.contains(collection)) {
                return true;
            }
        }

        return false;
    }
}
