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

import com.blobcity.db.cluster.messaging.ClusterMessaging;
import com.blobcity.db.transientstate.TransientStore;
import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.query.Query;
import com.blobcity.pom.database.engine.factory.EngineBeanConfig;
import org.springframework.context.ApplicationContext;

/**
 * @author sanketsarang
 */
public abstract class AbstractCommitProcessor {

    protected final ApplicationContext applicationContext;
    protected final Query query;
    protected final TransientStore transientStore;
    protected boolean rollbackNeedsAction = false; //should be set to true if soft commit was successful

    public AbstractCommitProcessor(final Query query) {
        this.query = query;
        this.applicationContext = BeanConfigFactory.getConfigBean(EngineBeanConfig.class.getName());
        this.transientStore = applicationContext.getBean(TransientStore.class);
    }

    protected <T> T getBean(Class<T> clazz) {
        return this.applicationContext.getBean(clazz);
    }

    protected TransientStore getTransientStore() {
        return getBean(TransientStore.class);
    }

    protected ClusterMessaging getClusterMessagingBean() {
        return getBean(ClusterMessaging.class);
    }

    protected void setRollbackNeedsAction() {
        rollbackNeedsAction = true;
    }

    protected boolean doesRollbackNeedAction() {
        return rollbackNeedsAction;
    }
}
