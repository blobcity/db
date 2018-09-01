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

package com.blobcity.db.master.executors.data;

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.executors.generic.ExecuteSelectedNodesCommitMaster;
import com.blobcity.db.memory.records.*;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import com.blobcity.lib.query.QueryType;
import com.blobcity.lib.query.RecordType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Master that manages the data insert requests
 *
 * @author sanketsarang
 */
public class SelectMaster extends ExecuteSelectedNodesCommitMaster implements MasterExecutable {

    public SelectMaster(Query query) throws OperationException {
        super(query, ClusterNodesStore.getInstance().getLeastLoadedNodes(SchemaStore.getInstance().getReplicationFactor(query.getDs(), query.getCollection())));

        if(query.getQueryType() != QueryType.SELECT && query.getQueryType() != QueryType.SEARCH) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Incorrect query passed to insert master");
        }
    }

    @Override
    public Query call() throws Exception {
        List<com.blobcity.lib.data.Record> toInsertList = new ArrayList<>();

        final String ds = super.query.getString(QueryParams.DATASTORE);
        final String collection = super.query.getString(QueryParams.COLLECTION);

        BSqlCollectionManager collectionManager = super.getBean(BSqlCollectionManager.class);
        if(!collectionManager.exists(ds, collection)) {
            return new Query().ackFailure().errorCode(ErrorCode.COLLECTION_INVALID.getErrorCode());
        }

        final JSONObject payloadJson = super.query.getJSONObject(QueryParams.PAYLOAD);
        final RecordType recordType = RecordType.fromTypeCode(payloadJson.getString(QueryParams.TYPE.getParam()));
        final JSONArray recordsArray = payloadJson.getJSONArray(QueryParams.DATA.getParam());
        final List<Object> records = new ArrayList<>();
        for(int i = 0; i < recordsArray.length(); i++) {
            records.add(recordsArray.get(i));
        }

        List<String> csvColumnNames = null; //only for CSV type
        if(recordType == RecordType.CSV && payloadJson.has(QueryParams.COLS.getParam())) {
            csvColumnNames = (List<String>) payloadJson.get(QueryParams.COLS.getParam());
        }

        for(Object r : records) {
            RecordType rt;
            if(recordType == RecordType.AUTO) {
                rt = RecordInterpreter.getBestMatchedType(r);
            } else {
                rt = recordType;
            }

            switch (rt) {
                case JSON:
                    toInsertList.add(new JsonRecord(r.toString()));
                    break;
                case XML:
                    toInsertList.add(new XmlRecord(r.toString()));
                    break;
                case SQL:
                    toInsertList.add(new SqlRecord(r.toString()));
                    break;
                case TEXT:
                    toInsertList.add(new TextRecord(r.toString()));
                    break;
                case CSV:
                    if(csvColumnNames == null) {
                        try {
                            toInsertList.add(new CsvRecord(r.toString(), SchemaStore.getInstance().getSchema(ds, collection)));
                        } catch (OperationException e) {
                            toInsertList.add(null);
                        }
                    } else {
                        toInsertList.add(new CsvRecord(csvColumnNames, Arrays.asList(r.toString().split(","))));
                    }
                    break;
            }
        }

        //TODO: Create missing columns here

        super.query.insertQuery(ds, collection, toInsertList, recordType);
        this.messageAllConcernedNodes(super.query);
        this.awaitCompletion(); //TODO: Might want to have a timeout to prevent indefinite waiting
        return this.getResponse();
    }
}
