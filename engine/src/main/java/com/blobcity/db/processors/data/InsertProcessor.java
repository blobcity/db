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

package com.blobcity.db.processors.data;

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.memory.records.*;
import com.blobcity.db.processors.AbstractCommitProcessor;
import com.blobcity.db.processors.Processor;
import com.blobcity.db.requests.RequestHandlingBean;
import com.blobcity.db.transaction.CollectionCommitLogWriter;
import com.blobcity.lib.query.CollectionStorageType;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author sanketsarang
 */
public class InsertProcessor extends AbstractCommitProcessor implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(InsertProcessor.class.getName());

    private final String ds;
    private final String collection;
    private final List<String> pkList = Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> statusList = Collections.synchronizedList(new ArrayList<>());

    public InsertProcessor(final Query query) {
        super(query);

        this.ds = super.query.getString(QueryParams.DATASTORE);
        this.collection = super.query.getString(QueryParams.COLLECTION);
    }

    @Override
    public void softCommit() {
        Query responseQuery;

        BSqlCollectionManager collectionManager = super.getBean(BSqlCollectionManager.class);
        if(!collectionManager.exists(this.ds, this.collection)) {
            logger.debug(query.getRequestId() + " : No collection found with name " + this.ds + "." + this.collection);
            sendSoftCommitFailure();
            return;
        }

        //up to this point, the operation performed will not reflect in commit logs for the table

        CollectionCommitLogWriter commitLogWriter = super.getBean(CollectionCommitLogWriter.class);
        try {
            commitLogWriter.write(this.ds, this.collection, query);
        } catch (OperationException e) {
            logger.debug(query.getRequestId() + " : " + e.getErrorCode().getErrorCode() + " - " + e.getErrorCode().getErrorMessage());
            sendSoftCommitFailure();
            return;
        }

        final JSONObject payloadJson = super.query.getJSONObject(QueryParams.PAYLOAD);
        final JSONArray recordsArray = payloadJson.getJSONArray(QueryParams.DATA.getParam());
        final List<JsonRecord> records = new ArrayList<>();

        for(int i = 0; i < recordsArray.length(); i++) {
            records.add(new JsonRecord(recordsArray.getJSONObject(i)));
        }

        BSqlDataManager dataManager = super.getBean(BSqlDataManager.class);

        records.parallelStream().forEach(record -> {
            pkList.add(record.getId());

            try {
                if(super.transientStore.acquireRecordPermit(ds, collection, record.getId())) { //permit will be released post commit or rollback
                    dataManager.insert(this.ds, this.collection, record);
                    statusList.add(1);
                    super.setRollbackNeedsAction();
                } else {
                    statusList.add(0);
                }
            } catch (OperationException e) {
                logger.debug(query.getRequestId() + " : " + e.getErrorCode().getErrorCode() + " - " + e.getErrorCode().getErrorMessage());
                statusList.add(0);
            }
        });

        responseQuery = new Query().requestId(query.getRequestId()).softCommitSuccessQuery().ackSuccess();
        responseQuery.put(QueryParams.STATUS, statusList);
        try {
            commitLogWriter.write(ds, collection, responseQuery);
            super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
        } catch (OperationException e) {
            logger.debug(query.getRequestId() + " : " + e.getErrorCode().getErrorCode() + " - " + e.getErrorCode().getErrorMessage());
            records.parallelStream().forEach(record -> super.transientStore.releaseRecordPermit(this.ds, this.collection, record.getId()));
            sendSoftCommitFailure();
        }
    }

    @Override
    public void commit() {

        /* Non-safe parallel implementation, that assumes insert will always succeed in single node operations */
        pkList.parallelStream().forEach(pk -> super.transientStore.releaseRecordPermit(this.ds, this.collection, pk));

        /* Original implementation that takes into account insert status */
//        for(int i = 0; i < statusList.size(); i++) {
//            if(statusList.get(i) == 1) {
//                super.transientStore.releaseRecordPermit(this.ds, this.collection, pkList.get(i));
//            }
//        }

        try {
            Query responseQuery = new Query().requestId(query.getRequestId()).commitSuccessQuery().ack("1");
            super.getBean(CollectionCommitLogWriter.class).write(this.ds, this.collection, responseQuery);
            super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
        } catch (OperationException e) {
            logger.debug(query.getRequestId() + " : " + e.getErrorCode().getErrorCode() + " - " + e.getErrorCode().getErrorMessage());
            sendCommitFailure();
        }

    }

    @Override
    public void rollback() {
        Query responseQuery = new Query().requestId(query.getRequestId()).rollbackSuccessQuery().ack("1");

        try {
            if(doesRollbackNeedAction()) {
                BSqlDataManager dataManager = super.getBean(BSqlDataManager.class);

                for(int i = 0; i < statusList.size(); i++) {
                    if(statusList.get(i) == 1) {
                        dataManager.remove(this.ds, this.collection, pkList.get(i));
                        super.transientStore.releaseRecordPermit(this.ds, this.collection, pkList.get(i));
                    }
                }
            }
        }catch(OperationException e) {
            //do nothing, as the rollback should auto execute at sometime in the future
            logger.debug(query.getRequestId() + " : " + e.getErrorCode().getErrorCode() + " - " + e.getErrorCode().getErrorMessage());
        }

        try {
            super.getBean(CollectionCommitLogWriter.class).write(this.ds, this.collection, responseQuery);
        } catch (OperationException e) {
            //do nothing, as the rollback should auto execute at sometime in the future
            logger.debug(query.getRequestId() + " : " + e.getErrorCode().getErrorCode() + " - " + e.getErrorCode().getErrorMessage());
        }

        super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
    }

    private void sendSoftCommitFailure() {
        super.getClusterMessagingBean().sendMessage(
                new Query().requestId(query.getRequestId()).softCommitSuccessQuery().ackFailure(),
                query.getMasterNodeId());
    }

    private void sendCommitFailure() {
        super.getClusterMessagingBean().sendMessage(
                new Query().requestId(query.getRequestId()).commitSuccessQuery().ackFailure(),
                query.getMasterNodeId());
    }

}
