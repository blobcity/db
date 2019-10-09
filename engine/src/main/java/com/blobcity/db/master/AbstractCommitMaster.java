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

package com.blobcity.db.master;

import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.cluster.messaging.ClusterMessaging;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.features.FeatureRules;
import com.blobcity.db.transaction.TransactionPhase;
import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import com.blobcity.pom.database.engine.factory.EngineBeanConfig;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.mina.util.ConcurrentHashSet;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * Contains base functionality for getting the current cluster status and checking for cluster quorum before proceeding
 * with request execution
 *
 * @author sanketsarang
 */
public abstract class AbstractCommitMaster implements MasterExecutable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCommitMaster.class);

    public static final Set<String> activeQueries = new ConcurrentHashSet<>();

    protected final Query query;
    protected final ApplicationContext applicationContext;
    protected final Semaphore semaphore = new Semaphore(0);
    protected boolean completed = false;
    protected Query response;
    protected TransactionPhase transactionPhase = TransactionPhase.SOFT_COMMIT;
    protected Map<String, Boolean> successMap = new ConcurrentHashMap<>();
    protected Map<String, ErrorCode> errorCodeMap = new ConcurrentHashMap<>();
    protected Map<String, Long> pingMap = null;
    protected Set<String> nodeIds = null;

    public AbstractCommitMaster(Query query) {
        this.query = query;
        this.applicationContext = BeanConfigFactory.getConfigBean(EngineBeanConfig.class.getName());
    }

    public AbstractCommitMaster(Query query, Set<String> nodeIds) {
        this(query);
        this.nodeIds = nodeIds;
    }

    protected ClusterMessaging clusterMessagingBeanInstance() {
        return this.applicationContext.getBean(ClusterMessaging.class);
    }

//    protected void awaitCompletion() {
//        semaphore.acquireUninterruptibly(); //waits till a release occurs externally
//        semaphore.release(); // release the immediately last acquire, as the semaphore is no longer required
//    }

    protected void awaitCompletion() {
        try {
            boolean acquired = semaphore.tryAcquire(FeatureRules.COMMIT_OP_TIMEOUT, TimeUnit.SECONDS); //waits 60 seconds
            if(!acquired) {
                logger.warn("Request (" + query.getRequestId() + ") timed out while attempting to commit transaction");
                rollback();
                try {
                    semaphore.tryAcquire(FeatureRules.COMMIT_OP_TIMEOUT, TimeUnit.SECONDS); //waits another 60 seconds for rollback
                } catch (InterruptedException e1) {
                    logger.warn("Request (" + query.getRequestId() + ") rollback interrupted after commit timeout");
                    complete(new Query().ack("0").errorCode("INTERNAL-ERROR with transaction handling"));
                }
            }
        } catch (InterruptedException e) {
            logger.warn("Request (" + query.getRequestId() + ") interrupted while waiting for commit");
            rollback();
            try {
                semaphore.tryAcquire(FeatureRules.COMMIT_OP_TIMEOUT, TimeUnit.SECONDS); //waits another 60 seconds for rollback
            } catch (InterruptedException e1) {
                logger.warn("Request (" + query.getRequestId() + ") rollback interrupted after commit interruption");
                complete(new Query().ack("0").errorCode("INTERNAL-ERROR with transaction handling"));
            }
        } finally {
            semaphore.release();
        }
    }

    protected void complete(Query query) {
        this.response = query;
        this.completed = true;
        this.semaphore.release();
    }

    protected void complete(JSONObject responseJson) {
        this.response = new Query(responseJson);
        this.completed = true;
        this.semaphore.release();
    }

    protected Query getResponse() {
        return this.response;
    }

    protected void ping(final String nodeId) {
        if(pingMap == null) {
            pingMap = new HashMap<>(); //prevents creation of map for quries that execute in less than first ping interval
        }

        pingMap.put(nodeId, System.currentTimeMillis());
    }

    protected void messageAllConcernedNodes(Query query) {
        if(this.nodeIds == null) {
            this.clusterMessagingBeanInstance().sendMessage(query); //sends to all nodes in the cluster. May use UDP.
        } else {
            this.clusterMessagingBeanInstance().sendMessage(query, nodeIds); //send to specified nodes. Uses TCP.
        }
    }

    @Override
    public void notifyMessage(final String nodeId, final Query query) {
        switch(query.getQueryType()) {
            case SOFT_COMMIT_SUCCESS:
                registerSuccessStatus(nodeId, query);
                if(allSuccess()) {
                    commit();
                    return;
                } else if(didAllRespond()) {
                    rollback();
                    return;
                }
                break;
            case COMMIT_SUCCESS:
                registerSuccessStatus(nodeId, query);
                if(allSuccess()) {
                    complete(new Query().ack("1"));
                    return;
                } else if(didAllRespond()) {
                    rollback();
                    return;
                }
                break;
            case ROLLBACK_SUCCESS:
                registerSuccessStatus(nodeId, query);
                if(didAllRespond()) {
                    complete(new Query().ack("0").errorCode(getErrorCode()));
                    return;
                }
                break;
        }
    }

    @Override
    public Query call() throws Exception {
        this.messageAllConcernedNodes(this.query);
        this.awaitCompletion(); //TODO: Might want to have a timeout to prevent indefinite waiting
        return this.getResponse();
    }

    @Override
    public void rollback() {
        successMap.clear();
        transactionPhase = TransactionPhase.ROLLBACK;
        messageAllConcernedNodes(new Query().requestId(this.query.getRequestId()).rollbackQuery());
    }

    protected boolean allSuccess() {
        if(!didAllRespond()) {
            return false;
        }

        Collection<Boolean> values = successMap.values();
        for(Boolean value : values) {
            if(!value) {
                return false;
            }
        }

        return true;
    }

    protected boolean didAllRespond() {
        if(successMap == null) {
            return false;
        }

        Set<String> nodesToCheck;
        if(nodeIds == null) {
            nodesToCheck = ClusterNodesStore.getInstance().getAllNodes();
        } else {
            nodesToCheck = nodeIds;
        }

        for(String nodeId : nodesToCheck) {
            if(!successMap.containsKey(nodeId)) {
                return false;
            }
        }

        return true;
    }

    protected void registerSuccessStatus(final String nodeId, Query query) {
        switch(query.getQueryType()) {
            case SOFT_COMMIT_SUCCESS:
                if(transactionPhase == TransactionPhase.SOFT_COMMIT) {
                    successMap.put(nodeId, query.isAckSuccess());

                    if(!query.isAckSuccess() && query.contains(QueryParams.ERROR_CODE) && query.getErrorCode() != null) {
                        errorCodeMap.put(nodeId, ErrorCode.fromString(query.getErrorCode()));
                    }
                }
                break;
            case COMMIT_SUCCESS:
                if(transactionPhase == TransactionPhase.COMMIT) {
                    successMap.put(nodeId, query.isAckSuccess());

                    if(!query.isAckSuccess() && query.contains(QueryParams.ERROR_CODE) && query.getErrorCode() != null) {
                        errorCodeMap.put(nodeId, ErrorCode.fromString(query.getErrorCode()));
                    }
                }
                break;
            case ROLLBACK_SUCCESS:
                if(transactionPhase == TransactionPhase.ROLLBACK) {
                    successMap.put(nodeId, query.isAckSuccess());

                    if(!query.isAckSuccess() && query.getErrorCode() != null) {
                        errorCodeMap.put(nodeId, ErrorCode.fromString(query.getErrorCode()));
                    }
                }
                break;
        }
    }

    protected void commit() {
        successMap.clear();
        transactionPhase = TransactionPhase.COMMIT;
        messageAllConcernedNodes(new Query().requestId(this.query.getRequestId()).commitQuery());
    }

    protected boolean hasErrors() {
        return !errorCodeMap.isEmpty();
    }

    protected String getErrorCode() {
        if(!hasErrors()) {
            return null;
        }

        return errorCodeMap.values().toArray()[0].toString();
    }

    protected <T> T getBean(Class<T> clazz) {
        return this.applicationContext.getBean(clazz);
    }

    public void processSoftCommitSuccessMessage(final String nodeId, final Query query) {

    }
}
