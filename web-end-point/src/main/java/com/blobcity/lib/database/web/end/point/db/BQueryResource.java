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

package com.blobcity.lib.database.web.end.point.db;

import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.database.bean.manager.interfaces.engine.BQueryExecutor;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;

import javax.validation.constraints.Size;
import javax.ws.rs.FormParam;
import javax.ws.rs.Path;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;

import com.blobcity.lib.database.bean.manager.interfaces.security.SecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * REST Web Service
 *
 * @author sanketsarang
 */
@Path("rest/bquery")
public class BQueryResource {

    private final Logger logger;
    private final BQueryExecutor bQueryExecutor;
    private final SecurityManager securityManager;
    private final RequestStore requestStore;

    public BQueryResource() {
        this.logger = LoggerFactory.getLogger(BQueryResource.class.getName());
        ApplicationContext context = BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig");
        this.bQueryExecutor = (BQueryExecutor) context.getBean("BQueryExecutorBean");
        this.requestStore = (RequestStore) context.getBean("RequestStoreBean");
        this.securityManager = (SecurityManager) context.getBean("SecurityManagerBean");
    }

    /**
     * Retrieves representation of an instance of com.blobcity.db.BQueryResource
     *
     * @param query
     * @return an instance of java.lang.String
     */
    //@GET
    @POST
    @Produces("application/json")
//    @Size(max = Integer.MAX_VALUE)
    public String getJson(@FormParam(value = "username")
            final String username,
            @FormParam(value = "password")
            final String password,
            @FormParam(value = "key")
            final String apiKey,
            @FormParam(value = "db")
            final String db,
            @FormParam("q")
            final String query) {
        logger.trace("Received DB query: {}", query);
        final long startTime = System.currentTimeMillis();

        if (query == null || query.isEmpty()) {
            return "{\"ack\":\"0\", \"cause\":\"Invalid query format\"}";
        }

        if(apiKey != null) {
            if(!securityManager.verifyKey(apiKey)) {
                return "{\"ack\":\"0\", \"cause\":\"Invalid credentials\"}";
            }
        } else if(!securityManager.verifyCredentials(username, password)) {
            return "{\"ack\":\"0\", \"cause\":\"Invalid credentials\"}";
        }

        final String response = bQueryExecutor.runQuery(query);
        final long executionTime = System.currentTimeMillis() - startTime;
        logger.trace("Response to DB query: {}", response);
        logger.trace("Execution time (ms): " + executionTime);
        return response;
    }
}
