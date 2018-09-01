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

import com.blobcity.code.ExportServiceRouter;
import com.blobcity.code.WebServiceExecutor;
import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.database.bean.manager.interfaces.engine.BQueryExecutor;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import com.blobcity.lib.database.bean.manager.interfaces.security.SecurityManager;
import com.blobcity.lib.export.ExportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

/**
 * Multiformat export service. The endpoints must route to a stored procedure that is designed to export data of the
 * desired format
 *
 * @author sanketsarang
 */

@Path("export/{ds}/{sp-name}/{export-type}")
public class ExportService {

    private final Logger logger;
    private final BQueryExecutor bQueryExecutor;
    private final SecurityManager securityManager;
    private final RequestStore requestStore;
    private final WebServiceExecutor webServiceExecutor;
    private final ExportServiceRouter exportServiceRouter;

    public ExportService() {
        this.logger = LoggerFactory.getLogger(ExportService.class.getName() + ":" + System.currentTimeMillis());
        ApplicationContext context = BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig");
        this.bQueryExecutor = (BQueryExecutor) context.getBean("BQueryExecutorBean");
        this.requestStore = (RequestStore) context.getBean("RequestStoreBean");
        this.securityManager = (SecurityManager) context.getBean("SecurityManagerBean");
        this.webServiceExecutor = (WebServiceExecutor) context.getBean("WebServiceExecutor");
        this.exportServiceRouter = (ExportServiceRouter) context.getBean("ExportServiceRouter");
    }

    @GET
    @Produces("application/octet-stream")
    public Response handleGet(
            @PathParam(value ="ds") final String datastore,
            @PathParam(value = "sp-name") final String spName,
            @PathParam(value = "export-type") final String exportTypeString,
            @QueryParam("p") final String queryPayload,
            @PathParam("p") final String pathPayload) {
        logger.debug("Export service called [GET]: ds={}, sp-name:{}, export-type: {}", datastore, spName, exportTypeString);

        ExportType exportType = ExportType.fromTypeString(exportTypeString);

        if(exportType == null) {
            return Response.ok().header("Access-Control-Allow-Origin", "*").header("Content-disposition", "attachment; filename=" + "error.txt").entity(exportTypeString + " not a recognisable export type").build();
        }

        return Response.ok().header("Access-Control-Allow-Origin", "*").header("Content-disposition", "attachment; filename=" + "text.csv").entity("This is a test string. Params are: " + pathPayload + " | " + queryPayload).build();
    }


//    @POST
//    @Produces("application/json")
//    public Response handlePost(
//            @PathParam(value ="ds") final String datastore,
//            @PathParam(value = "version") final String version,
//            @PathParam(value = "url") final String url,
//            @QueryParam(value = "json") final String queryJson,
//            @FormParam(value = "json") final String formJson
//    ) {
//        final String wsPath = "/" + version + "/" + url;
//        logger.debug("Webservice called [POST]: " + wsPath);
//        JSONObject jsonRequest = queryJson != null ? new JSONObject(queryJson) : formJson != null ? new JSONObject(formJson) : new JSONObject();
//        return Response.ok().header("Access-Control-Allow-Origin", "*").entity(webServiceExecutor.executeGet(datastore, wsPath, jsonRequest).toString()).build();
//    }
}
