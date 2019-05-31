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

package com.blobcity.db.requests;

import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.cluster.messaging.ClusterMessaging;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.MasterExecutorService;
import com.blobcity.db.master.MasterStore;
import com.blobcity.db.master.executors.data.InsertMaster;
import com.blobcity.db.master.executors.schema.*;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import com.blobcity.lib.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author sanketsarang
 */
@Component
public class RequestHandlingBean {

    private static final Logger logger = LoggerFactory.getLogger(RequestHandlingBean.class.getName());

    @Autowired
    private RequestStore requestStore;
    @Autowired
    private ClusterNodesStore clusterNodesStore;
    @Autowired
    private MasterStore masterStore;
    @Autowired
    private ClusterMessaging clusterMessaging;

    public Query newRequest(Query query) {
        String requestId = null;

        try {

            /**
             * Creates a new request registered only on current node. The query object will contain the the requestId of
             * the new request after the operation.
             */
            requestId = requestStore.registerRequest(query);
            query.requestId(requestId);

//        logger.info("New Request: " + query.toJsonString());

            /* Set the masterNodeId on the query to the current node */
            query.masterNodeId(ClusterNodesStore.selfId);

            return processRequest(query);

        } finally {
            requestStore.unregisterRequest(requestId);
        }
    }

    public Query newSubRequest(final String parentRequestId, final Query query) {

        String requestId = null;

        try {

            /**
             * Sets the id of the parent request
             */
            query.parentRequestId(parentRequestId);

            /**
             * Creates a new request registered only on current node. The query object will contain the the requestId of
             * the new request after the operation.
             */
            requestId = requestStore.registerRequest(query);
            query.requestId(requestId);


            logger.info("New Sub Request for (" + parentRequestId + "): " + query.toJsonString());

            /* Set the masterNodeId on the query to the current node */
            query.masterNodeId(ClusterNodesStore.selfId);

            return processRequest(query);
        }finally {
            requestStore.unregisterRequest(requestId);
        }
    }

    private Query processRequest(final Query query) {
        MasterExecutable masterExecutable;

        try {
            switch (query.getQueryType()) {
//            case BULK_SELECT:
//                break;
//            case CONTAINS:
//                break;
//            case DELETE:
//                break;
                case INSERT:
                    masterExecutable = new InsertMaster(query);
                    break;
//            case SAVE:
//                break;
//            case SELECT:
//                break;
//            case SELECT_ALL:
//                break;
//            case UPDATE:
//                break;
                case CREATE_COLLECTION:
                case CREATE_TABLE:
                    masterExecutable = new CreateCollectionMaster(query);
                    break;
                case DROP_COLLECTION:
                case DROP_TABLE:
                    masterExecutable = new DropCollectionMaster(query);
                    break;
//            case VIEW_SCHEMA:
//                break;
                case LIST_COLLECTIONS:
                case LIST_TABLES:
                    masterExecutable = new ListCollectionMaster(query);
                    break;
//            case RENAME_COLLECTION:
//                break;
//            case RENAME_TABLE:
//                break;
//            case COLLECTION_EXISTS:
//                break;
//            case TABLE_EXISTS:
//                break;
//            case TRUNCATE_COLLECTION:
//                break;
//            case TRUNCATE_TABLE:
//                break;
//            case ADD_COLUMN:
//                break;
//            case ALTER_COLUMN:
//                break;
//            case CHANGE_DATA_TYPE:
//                break;
//            case DROP_COLUMN:
//                break;
//            case DROP_INDEX:
//                break;
//            case DROP_UNIQUE:
//                break;
//            case INDEX:
//                break;
//            case RENAME_COLUMN:
//                break;
//            case SEARCH:
//                break;
//            case SEARCH_AND:
//                break;
//            case SEARCH_AND_LOAD:
//                break;
//            case SEARCH_OR:
//                break;
//            case SEARCH_OR_LOAD:
//                break;
//            case SEARCH_FILTERED:
//                break;
//            case INSERT_CUSTOM:
//                break;
                case CREATE_DS:
                case CREATE_DB:
                    masterExecutable = new CreateDsMaster(query);
                    break;
                case DROP_DB:
                case DROP_DS:
                    masterExecutable = new DropDsMaster(query);
                    break;
                case LIST_DS:
                    masterExecutable = new ListDsMaster(query);
                    break;
//            case TRUNCATE_DS:
//                break;
//            case DS_EXISTS:
//                break;
//            case APPLY_LICENSE:
//                break;
//            case LICENSE_STATUS:
//                break;
//            case REVOKE_LICENSE:
//                break;
//            case BULK_EXPORT:
//                break;
//            case BULK_IMPORT:
//                break;
//            case LIST_OPS:
//                break;
//            case RESET_USAGE:
//                break;
//            case SET_LIMITS:
//                break;
//            case USAGE:
//                break;
//            case CHANGE_TRIGGER:
//                break;
//            case LIST_FILTERS:
//                break;
//            case LIST_TRIGGERS:
//                break;
//            case LOAD_CODE:
//                break;
//            case REGISTER_PROCEDURE:
//                break;
//            case REGISTER_TRIGGER:
//                break;
//            case UNREGISTER_PROCEDURE:
//                break;
//            case UNREGISTER_TRIGGER:
//                break;
//            case SP:
//                break;
//            case ADD_USER:
//                break;
//            case CHANGE_PASSWORD:
//                break;
//            case DROP_USER:
//                break;
//            case VERIFY_CREDENTIALS:
//                break;
//            case CREATE_GROUP:
//                break;
//            case DROP_GROUP:
//                break;
//            case ADD_TO_GROUP:
//                break;
//            case REMOVE_FROM_GROUP:
//                break;
//            case NODE_ID:
//                break;
//            case ADD_NODE:
//                break;
//            case LIST_NODES:
//                break;
//            case ROLLBACK:
//                break;
//            case COMMIT:
//                break;
//            case SOFT_COMMIT_SUCCESS:
//                break;
//            case COMMIT_SUCCESS:
//                break;
//            case ROLLBACK_SUCCESS:
//                break;
                default:
                    return new Query().errorCode(ErrorCode.INVALID_QUERY.getErrorCode())
                            .message(ErrorCode.INVALID_QUERY.getErrorMessage())
                            .ackFailure();
            }
        }catch(OperationException ex) {
            logger.error("Error occurred " + ex.getErrorCode() + " " + ex.getMessage());
            return new Query().ackFailure().errorCode(ex.getErrorCode().getErrorCode());
        }

        try {
            masterStore.register(query.getRequestId(), masterExecutable);
            Future<Query> futureResponse = MasterExecutorService.getInstance().submit(masterExecutable);

            Query response = futureResponse.get();
            if(response == null) {
                logger.warn("Request (" + query.getRequestId() + ") failed with unknown cause");
                masterExecutable.rollback();
                return new Query().ackFailure();
            }
            logger.info("Response for requestId: {}, response: {}", query.getRequestId(), response.toJsonString());
            return response;
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Request (" + query.getRequestId() + ") failed on an internal exception");
            masterExecutable.rollback();
            return new Query().ackFailure();
        } finally {
            masterStore.unregister(query.getRequestId());
        }
    }
}
