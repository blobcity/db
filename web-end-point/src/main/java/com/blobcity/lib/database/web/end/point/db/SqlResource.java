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
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import com.blobcity.lib.database.bean.manager.interfaces.engine.SqlExecutor;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StringUtils;

/**
 * Web end-point to run SQL queries
 *
 * @author javatarz (Karun Japhet)
 * @author sanketsarang
 */
@Path("rest/sql")
public class SqlResource {

    private final SqlExecutor sqlExecutorBean;
    private final RequestStore requestStore;
    private final Logger logger;

    public SqlResource() {
        this.logger = LoggerFactory.getLogger(SqlResource.class.getName() + ":" + System.currentTimeMillis());
        ApplicationContext context = BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig");
        this.sqlExecutorBean = context.getBean(SqlExecutor.class);
        this.requestStore = (RequestStore) context.getBean("RequestStoreBean");
    }

    @GET
    @Produces("application/json")
    public String getGetResponse(@QueryParam(value = "app") final String appId, @QueryParam(value = "q") final String queryPayload) {
        return "{ack:0,code:\"GET requests are not supported for SQL queries. Please send data using a POST request.\"}";
    }

//    @POST
//    @Produces("application/json")
//    public String getPostResponse(@FormParam(value = "app") final String appId, @FormParam(value = "q") final String queryPayload) {
//        final String response = sqlExecutorBean.runQuery(appId, queryPayload);
//        logger.debug("App ID: \"{}\"\nQuery: \"{}\"\n\nResponse: \"{}\"\n\nEnd of result.", new Object[]{appId, queryPayload, response});
//
//        return response;
//    }
    @POST
    @Produces("application/json")
    public Response getPostResponse(
            @FormParam(value = "username")
            final String username,
            @FormParam(value = "password")
            final String password,
            @FormParam(value = "ds")
            final String db,
            @FormParam(value = "q")
            final String queryPayload
    ) {
        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password) || StringUtils.isEmpty(db) || StringUtils.isEmpty(queryPayload)) {
            return Response.status(Response.Status.BAD_REQUEST).entity("All of the parameters: username, password, db, q - are required").build();
        }

        final long startTime = System.currentTimeMillis();
        final String response = sqlExecutorBean.runQuery("internal", username, password, db, queryPayload);
        final long executionTime = System.currentTimeMillis() - startTime;
        logger.debug("User: \"{}\"\n"
                + "DB: \"{}\"\n"
                + "Query: \"{}\"\n\n"
                + "Response: \"{}\"\n\n"
                + "End of result.", new Object[]{username, db, queryPayload, response});
        logger.debug("Execution time (ms): " + executionTime);

        return Response.ok(response, MediaType.APPLICATION_JSON).build();
    }
}
