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
import com.blobcity.db.master.aggregators.Aggregator;
import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import com.blobcity.pom.database.engine.factory.EngineBeanConfig;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author sanketsarang
 */
public abstract class AbstractReadMaster implements MasterExecutable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReadMaster.class);

    protected final Query query;
    protected final ApplicationContext applicationContext;
    protected final Semaphore semaphore = new Semaphore(0);
    protected boolean completed = false;
    protected Query response;
    protected Map<String, ErrorCode> errorCodeMap = new ConcurrentHashMap<>(0);
    protected Map<String, Long> pingMap = null;
    protected Set<String> nodeIds = null;
    protected Map<String, Query> responseMap = new ConcurrentHashMap<>(1); //set to number of nodes in cluster
    protected final Aggregator aggregator;

    public AbstractReadMaster(Query query, Aggregator aggregator) {
        this.query = query;
        this.applicationContext = BeanConfigFactory.getConfigBean(EngineBeanConfig.class.getName());
        this.aggregator = aggregator;
    }

    public AbstractReadMaster(Query query, Set<String> nodeIds, Aggregator aggregator) {
        this(query, aggregator);
        this.nodeIds = nodeIds;
    }

    protected ClusterMessaging clusterMessagingBeanInstance() {
        return this.applicationContext.getBean(ClusterMessaging.class);
    }

//    protected void awaitCompletion() {
//        semaphore.acquireUninterruptibly(); //waits till a release occurs externally
//        semaphore.release();
//    }

    protected void awaitCompletion() {
        try {
            boolean acquired = semaphore.tryAcquire(60, TimeUnit.SECONDS); //waits 60 seconds
            if(acquired) {
                semaphore.release(); // release the immediately last acquire, as the semaphore is no longer required
            } else {
                logger.warn("Request (" + query.getRequestId() + ") timed out while waiting for read to finish. Consider increasing READ_OP_TIMEOUT. Current value is: " + FeatureRules.READ_OP_TIMEOUT + " seconds");
                rollback();
            }
        } catch (InterruptedException e) {
            logger.warn("Request (" + query.getRequestId() + ") interrupted while waiting for read to finish");
            rollback();
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
            pingMap = new HashMap<>(); //prevents creation of map for queries that execute in less than first ping interval
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
            case QUERY_RESPONSE:
                registerResponse(nodeId, query);

                if(didAllRespond()) {
                    produceResponse();
                }
                break;
        }
    }

    @Override
    public void rollback() {

        //TODO: Also send other nodes a processing cancellation command, as they may continue running the read query

        complete(new Query().ackFailure());
        return;
    }

    @Override
    public Query call() throws Exception {
        this.messageAllConcernedNodes(this.query);
        this.awaitCompletion(); //TODO: Might want to have a timeout to prevent indefinite waiting
        return this.getResponse();
    }

    protected boolean allSuccess() {
        if(!didAllRespond()) {
            return false;
        }

        Collection<Query> values = responseMap.values();
        for(Query value : values) {
            if(!value.getAck().equalsIgnoreCase("1")) {
                return false;
            }
        }

        return true;
    }

    protected boolean didAllRespond() {
        Set<String> nodesToCheck;
        if(nodeIds == null) {
            nodesToCheck = ClusterNodesStore.getInstance().getAllNodes();
        } else {
            nodesToCheck = nodeIds;
        }

        for(String nodeId : nodesToCheck) {
            if(!responseMap.containsKey(nodeId)) {
                return false;
            }
        }

        return true;
    }

    protected void registerResponse(final String nodeId, final Query query) {
        responseMap.put(nodeId, query);
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

    protected void produceResponse() {
        if(!allSuccess()) {
            complete(new Query().ackFailure());
            return;
        }

        responseMap.forEach((key, value) -> {
//
//
//            JSONArray jsonArray = value.getJSONArray(QueryParams.PAYLOAD.getParam());
//            List<Object> list = new ArrayList<>();
//            for(int i = 0; i < jsonArray.length(); i++) {
//                list.add(jsonArray.get(i));
//            }
            aggregator.add((Collection)value.get(QueryParams.PAYLOAD.getParam()));
        });

        complete(new Query().ackSuccess().payload(aggregator.getAggregated()));
    }
}
