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

package com.blobcity.db.processors;

import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.MasterStore;
import com.blobcity.db.processors.data.InsertProcessor;
import com.blobcity.db.processors.schema.*;
import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.query.Query;
import com.blobcity.pom.database.engine.factory.EngineBeanConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * Executes the request using the appropriate execution functions. The class does minimilistic format and error checks
 * as it is assumed that the master has done all checks required to be carried out the query level and then sent
 * the request for direct execution.
 *
 * @author sanketsarang
 */
public class ProcessHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProcessHandler.class.getName());

    private final String nodeId;
    private final Query query;
    private final ApplicationContext applicationContext;

    public ProcessHandler(final String nodeId, final Query query) {
        this.nodeId = nodeId;
        this.query = query;
        this.applicationContext = BeanConfigFactory.getConfigBean(EngineBeanConfig.class.getName());
    }

    @Override
    public void run() {
        logger.debug("Received message from: {}, message: {}", nodeId, query.toJsonString());

        Processor processor;
        ProcessorStore processorStore;
        MasterStore masterStore;

        switch(query.getQueryType()) {

            /* Handle commands coming from master */
            case COMMIT:
                processorStore = getBean(ProcessorStore.class);
                processorStore.getAndUnregister(query.getRequestId()).commit();
                return;
            case ROLLBACK:
                processorStore = getBean(ProcessorStore.class);
                processorStore.getAndUnregister(query.getRequestId()).rollback();
                return;

            /* Handle commands coming to master from processing nodes */
            case COMMIT_SUCCESS:
            case SOFT_COMMIT_SUCCESS:
            case ROLLBACK_SUCCESS:
            case QUERY_RESPONSE:
                masterStore = getBean(MasterStore.class);
                MasterExecutable me = masterStore.get(query.getRequestId());
                me.notifyMessage(nodeId, query);
                return;

            /* Handle new query requests received by the processing nodes from the master */
            case CREATE_DS:
            case CREATE_DB:
                processorStore = getBean(ProcessorStore.class);
                processor = new CreateDsProcessor(query);
                processorStore.register(query.getRequestId(), processor);
                processor.softCommit();
                return;

            case DROP_DS:
            case DROP_DB:
                processorStore = getBean(ProcessorStore.class);
                processor = new DropDsProcessor(query);
                processorStore.register(query.getRequestId(), processor);
                processor.softCommit();
                return;

            case LIST_DS:
                processor = new ListDsProcessor(query);
                processor.softCommit();
                return;

            case CREATE_COLLECTION:
            case CREATE_TABLE:
                processorStore = getBean(ProcessorStore.class);
                processor = new CreateCollectionProcessor(query);
                processorStore.register(query.getRequestId(), processor);
                processor.softCommit();
                return;

            case DROP_COLLECTION:
            case DROP_TABLE:
                processorStore = getBean(ProcessorStore.class);
                processor = new DropCollectionProcessor(query);
                processorStore.register(query.getRequestId(), processor);
                processor.softCommit();
                return;

            case LIST_COLLECTIONS:
            case LIST_TABLES:
                processor = new ListCollectionProcessor(query);
                processor.softCommit();
                return;

            case INSERT:
                processorStore = getBean(ProcessorStore.class);
                processor = new InsertProcessor(query);
                processorStore.register(query.getRequestId(), processor);
                processor.softCommit();
                return;

        }
    }

    private <T> T getBean(Class<T> clazz) {
        return this.applicationContext.getBean(clazz);
    }
}
