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

import com.blobcity.code.WebServiceExecutor;
import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.database.bean.manager.interfaces.engine.BQueryExecutor;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import com.blobcity.lib.database.bean.manager.interfaces.security.SecurityManager;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

/**
 * REST Web Service
 *
 * @author sanketsarang
 */

@Path("ws/{ds}/{version}/{url}")
public class WebService {

    private final Logger logger;
    private final BQueryExecutor bQueryExecutor;
    private final SecurityManager securityManager;
    private final RequestStore requestStore;
    private final WebServiceExecutor webServiceExecutor;

    public WebService() {
        this.logger = LoggerFactory.getLogger(WebService.class.getName());
        ApplicationContext context = BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig");
        this.bQueryExecutor = (BQueryExecutor) context.getBean("BQueryExecutorBean");
        this.requestStore = (RequestStore) context.getBean("RequestStoreBean");
        this.securityManager = (SecurityManager) context.getBean("SecurityManagerBean");
        this.webServiceExecutor = (WebServiceExecutor) context.getBean("WebServiceExecutor");
    }

    @GET
    @Produces("application/json")
    public Response handleGet(
            @PathParam(value ="ds") final String datastore,
            @PathParam(value = "version") final String version,
            @PathParam(value = "url") final String url) {
        final String wsPath = "/" + version + "/" + url;
        logger.debug("Webservice called [GET]: " + wsPath);
        return Response.ok().header("Access-Control-Allow-Origin", "*").entity(webServiceExecutor.executeGet(datastore, wsPath, new JSONObject()).toString()).build();
    }


    /**
     * Accepts a POST request. The service accepts 2 variations for a POST request. One as a <code>form</code> parameter
     * and the other as a <code>json</code> parameter. A <code>json</code> parameter is given first preference and a
     * <code>form</code> parameter is given a second preference.
     * @param datastore
     * @param version
     * @param url
     * @param queryJson
     * @param formJson
     * @param formParamQuery
     * @param formParamJson
     * @return
     */
    @POST
    @Produces("application/json")
    public Response handlePost(
            @PathParam(value ="ds") final String datastore,
            @PathParam(value = "version") final String version,
            @PathParam(value = "url") final String url,
            @QueryParam(value = "json") final String queryJson,
            @FormParam(value = "json") final String formJson,
            @QueryParam(value = "form") final String formParamQuery,
            @FormParam(value = "form") final String formParamJson
    ) {
        final String wsPath = "/" + version + "/" + url;
        logger.debug("Webservice called [POST]: " + wsPath);
        JSONObject jsonRequest = queryJson != null ? new JSONObject(queryJson) : formJson != null ? new JSONObject(formJson) : new JSONObject();
        if(jsonRequest.isEmpty()) {
            jsonRequest = formParamQuery != null ? new JSONObject(formParamQuery) : formParamJson != null ? new JSONObject(formParamJson) : new JSONObject();
        }
        return Response.ok().header("Access-Control-Allow-Origin", "*").entity(webServiceExecutor.executeGet(datastore, wsPath, jsonRequest).toString()).build();
    }
}
