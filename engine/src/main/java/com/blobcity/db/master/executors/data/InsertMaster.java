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
import com.blobcity.db.code.CodeExecutor;
import com.blobcity.db.code.datainterpreter.InterpreterExecutorBean;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.executors.generic.ExecuteSelectedNodesCommitMaster;
import com.blobcity.db.memory.records.*;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.lib.data.Record;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryStore;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import com.blobcity.lib.query.QueryType;
import com.blobcity.lib.query.RecordType;
import org.json.JSONArray;
import org.json.JSONObject;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Operation;

import java.util.*;

/**
 * Master that manages the data insert requests
 *
 * @author sanketsarang
 */
public class InsertMaster extends ExecuteSelectedNodesCommitMaster implements MasterExecutable {

    private static final Logger logger = LoggerFactory.getLogger(InsertMaster.class.getName());
//    private static final Random random = new Random();

    private final InsertStatusHolder insertStatusHolder = new InsertStatusHolder();
    private final List<com.blobcity.lib.data.Record> toInsertList = new ArrayList<>();
    private long startTime;

    public InsertMaster(Query query) throws OperationException {
        super(query, ClusterNodesStore.getInstance().getLeastLoadedNodes(SchemaStore.getInstance().getReplicationFactor(query.getDs(), query.getCollection())));

        if(query.getQueryType() != QueryType.INSERT) {
            logger.debug(query.getRequestId() + " : " + ErrorCode.INTERNAL_OPERATION_ERROR.getErrorCode() + " - " + "Incorrect query passed to insert master");
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Incorrect query passed to insert master");
        }
    }

    @Override
    public Query call() throws Exception {
        startTime = System.currentTimeMillis();

        final String ds = super.query.getString(QueryParams.DATASTORE);
        final String collection = super.query.getString(QueryParams.COLLECTION);

        QueryStore queryStore = super.getBean(QueryStore.class);
        RequestStore requestStore = super.getBean(RequestStore.class);

        BSqlCollectionManager collectionManager = super.getBean(BSqlCollectionManager.class);
        if(!collectionManager.exists(ds, collection)) {
            logger.debug(query.getRequestId() + " : " + "No collection found with name " + ds + "." + collection);
            return new Query().ackFailure().errorCode(ErrorCode.COLLECTION_INVALID.getErrorCode());
        }

        insertStatusHolder.setReplicationFactor(SchemaStore.getInstance().getReplicationFactor(ds, collection));

        final JSONObject payloadJson = super.query.getJSONObject(QueryParams.PAYLOAD);
//        final RecordType recordType = RecordType.fromTypeCode(payloadJson.getString(QueryParams.TYPE.getParam()));
        final JSONArray recordsArray = payloadJson.getJSONArray(QueryParams.DATA.getParam());
        List<Object> records = new ArrayList<>();
        for(int i = 0; i < recordsArray.length(); i++) {
            records.add(recordsArray.get(i));
        }

        final String interpreterName = super.query.contains(QueryParams.INTERPRETER) ? super.query.getString(QueryParams.INTERPRETER) : null;
        final String interceptorName = super.query.contains(QueryParams.INTERCEPTOR) ? super.query.getString(QueryParams.INTERCEPTOR) : null;

        final RecordType recordType = interpreterName != null || interceptorName != null ? RecordType.JSON : RecordType.fromTypeCode(payloadJson.getString(QueryParams.TYPE.getParam()));


        /* Check for column names only for CSV type of data */
        List<String> csvColumnNames = null; //only for CSV type
        if(recordType == RecordType.CSV && payloadJson.has(QueryParams.COLS.getParam())) {
            csvColumnNames = (List<String>) payloadJson.get(QueryParams.COLS.getParam());
        }

        /* Check for specified interpreter if insert is of text type */
//        String interpreterName = null;
//        if(recordType == RecordType.TEXT && super.query.contains(QueryParams.INTERPRETER)) {
//            interpreterName = super.query.getString(QueryParams.INTERPRETER);
//        }

        /* Run interpreter if defined when inserted text records */
        if(interpreterName != null) { //this will always be null if record type is not text
            try {
                records = runInterpreter(interpreterName, records);
            } catch (OperationException ex) {
                return produceErrorResponse(ex.getErrorCode());
            }
        }

        /* Check for specified interceptor */
//        String interceptorName = null;
//        if(super.query.contains(QueryParams.INTERCEPTOR)) {
//            interceptorName = super.query.getString(QueryParams.INTERCEPTOR);
//        }

        if(interceptorName != null) {
            try {
                records = runInterceptor(interceptorName, records);
            } catch(OperationException ex) {
                return produceErrorResponse(ex.getErrorCode());
            }
        }

        final Schema schema = SchemaStore.getInstance().getSchema(ds, collection);

        final List<String> csvColumnNamesFinal = csvColumnNames;
        records.forEach(r -> {
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
                    if(csvColumnNamesFinal == null) {
                        toInsertList.add(new CsvRecord(r.toString(), schema));
                    } else {
                        toInsertList.add(new CsvRecord(csvColumnNamesFinal, Arrays.asList(r.toString().split(","))));
                    }
                    break;
            }
        });

//        for(Object r : records) {
//            RecordType rt;
//            if(recordType == RecordType.AUTO) {
//                rt = RecordInterpreter.getBestMatchedType(r);
//            } else {
//                rt = recordType;
//            }
//
//            switch (rt) {
//                case JSON:
//                    toInsertList.add(new JsonRecord(r.toString()));
//                    break;
//                case XML:
//                    toInsertList.add(new XmlRecord(r.toString()));
//                    break;
//                case SQL:
//                    toInsertList.add(new SqlRecord(r.toString()));
//                    break;
//                case TEXT:
//                    toInsertList.add(new TextRecord(r.toString()));
//                    break;
//                case CSV:
//                    if(csvColumnNames == null) {
//                        toInsertList.add(new CsvRecord(r.toString(), schema));
//                    } else {
//                        toInsertList.add(new CsvRecord(csvColumnNames, Arrays.asList(r.toString().split(","))));
//                    }
//                    break;
//            }
//        }

        //TODO: Create missing columns here

//        for(Record record : toInsertList) {
//            record.asJson().keys().forEachRemaining(key -> {
//
//            });
//        }


        insertStatusHolder.addRecords(ClusterNodesStore.getInstance().getSelfId(), toInsertList); //this needs to change for smart sharding
        super.query.insertQuery(ds, collection, toInsertList, recordType);
//        this.acquireSemaphore();
        this.messageAllConcernedNodes(super.query);

//        int num = random.nextInt(1000);
//        System.out.println("A: " + num + " " + queryStore.size("test") +  " " + requestStore.size());
        this.awaitCompletion(); //TODO: Might want to have a timeout to prevent indefinite waiting
//        System.out.println("C: " + num + " " + queryStore.size("test") + " " +  requestStore.size());
        return this.getResponse();
    }

    public void notifyMessage(final String nodeId, final Query query) {
        switch(query.getQueryType()) {
            case SOFT_COMMIT_SUCCESS:
                registerSuccessStatus(nodeId, query);

                if(query.isAckSuccess()) {
                    Object statusArray = query.get(QueryParams.STATUS);
                    List<Integer> statusList;

                    if(statusArray instanceof List) {
                        statusList = (List<Integer>) statusArray;
                    } else {
                        statusList = (List<Integer>) Collections2.transform(Arrays.asList(((JSONArray)statusArray).join(",").split(",")), new Function<String, Integer>() {
                            public Integer apply(String str) {
                                return Integer.parseInt(str.toString());
                            }
                        });
                    }

                    insertStatusHolder.addStatus(nodeId, statusList);
                } else {
                    logger.debug(query.getRequestId() + " : " + "Soft commit failure on node " + nodeId + ". Internal query response: " + query.toJsonString());
                }

                if(allSuccess()) {
                    if(insertStatusHolder.allInsertsConsistent()) {
                        commit();
                    } else {
                        logger.debug(query.getRequestId() + " : " + "Rolling back on SOFT_COMMIT_SUCCESS as inserts on all nodes are not consistent");
                        rollback();
                    }
                    return;
                } else if(didAllRespond()) {
                    logger.debug(query.getRequestId() + " : " + "Rolling back on SOFT_COMMIT_SUCCESS as some nodes responded with failure");
                    rollback();
                    return;
                }
                break;
            case COMMIT_SUCCESS:
                registerSuccessStatus(nodeId, query);
                if(allSuccess()) {
                    complete(produceFinalResponse());
                    insertStatusHolder.invalidate();
                    return;
                } else if(didAllRespond()) {
                    logger.debug(query.getRequestId() + " : " + "Rolling back on COMMIT_SUCCESS as some nodes responded with failure");
                    rollback();
                    return;
                }
                break;
            case ROLLBACK_SUCCESS:
                registerSuccessStatus(nodeId, query);
                if(didAllRespond()) {
                    complete(new Query().ack("0").errorCode(getErrorCode()));
                    insertStatusHolder.invalidate();
                    return;
                }
                break;
        }
    }

    private Query produceFinalResponse() {
        Query responseQuery = new Query().ackSuccess();
        responseQuery.payload(insertStatusHolder.produceResponsePayload(toInsertList));
        responseQuery.time(System.currentTimeMillis() - startTime);
        return responseQuery;
    }

    private List<Object> runInterpreter(final String interpreter, final List<Object> records) throws OperationException {
        List<Object> responseList = new ArrayList<>();
        CodeExecutor codeExecutor = super.getBean(CodeExecutor.class);

        for(Object obj : records) {
            responseList.add(codeExecutor.executeDataInterpreter(super.query.getDs(), interpreter, obj.toString()));
        }

        return responseList;
    }

    private List<Object> runInterceptor(final String interceptor, final List<Object> records) throws OperationException {


        throw new UnsupportedOperationException("Interceptors not supported yet.");
    }

    private Query produceErrorResponse(ErrorCode errorCode) {
        return new Query().ackFailure().errorCode(errorCode.getErrorCode());
    }
}
