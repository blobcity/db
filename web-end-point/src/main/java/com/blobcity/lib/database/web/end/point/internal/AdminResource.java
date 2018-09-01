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

package com.blobcity.lib.database.web.end.point.internal;

import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.database.bean.manager.interfaces.engine.BQueryExecutor;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import javax.ws.rs.FormParam;
import javax.ws.rs.Path;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * REST Web Service
 *
 * @author sanketsarang
 */
@Path("rest/admin")
public class AdminResource {

    private static final Logger logger = LoggerFactory.getLogger(AdminResource.class);
    private final BQueryExecutor bQueryAdminBean;
    private final RequestStore requestStore;
    
    public AdminResource() {
        ApplicationContext context = BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig");
        this.bQueryAdminBean = (BQueryExecutor) context.getBean("BQueryAdminBean");
        this.requestStore = (RequestStore) context.getBean("RequestStoreBean");
    }

    /**
     * Retrieves representation of an instance of com.blobcity.db.web.internal.AdminResource
     *
     * @param query
     * @return an instance of java.lang.String
     */
    @POST
    @Produces("application/json")
    public String getJson(@FormParam(value = "username")
            final String username,
            @FormParam(value = "password")
            final String password,
            @FormParam(value = "db")
            final String db,
            @FormParam("q") String query) {
        logger.trace("Received Admin query: {}", query);
//        final String requestId = requestStore.registerNewRequest(db, username, password, null);
        final String response = bQueryAdminBean.runQuery(query);
        logger.trace("Response to Admin query: {}", response);
//        requestStore.unregisterRequest(requestId);
        return response;
    }
}
