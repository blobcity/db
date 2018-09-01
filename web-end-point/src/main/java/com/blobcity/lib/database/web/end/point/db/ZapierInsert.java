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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.*;

/**
 * REST Web Service
 *
 * @author sanketsarang
 */
@Path("rest/zap-insert")
public class ZapierInsert {

    private final Logger logger;
    private final BQueryExecutor bQueryExecutor;
    private final RequestStore requestStore;
//    private final com.blobcity.db.requests.RequestHandlingBean requestHandlingBean;

    public ZapierInsert() {
        this.logger = LoggerFactory.getLogger(ZapierInsert.class.getName() + ":" + System.currentTimeMillis());
        ApplicationContext context = BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig");
        this.bQueryExecutor = (BQueryExecutor) context.getBean("BQueryExecutorBean");
        this.requestStore = (RequestStore) context.getBean("RequestStoreBean");
    }

    /**
     * Retrieves representation of an instance of com.blobcity.db.BQueryResource
     *
     * @param query
     * @return an instance of java.lang.String
     */
//    @GET
    @POST
    @Produces("application/json")
    public String getJson(@QueryParam("username")
            final String username,
            @QueryParam("password")
            final String password,
            @QueryParam("ds")
            final String ds,
                          final String body) {

        final JSONObject responseJson = new JSONObject();

        logger.trace("Username: {}", username);

        System.out.println("Username: " + username);
        System.out.println("Password: " + password);
        System.out.println("ds: " + ds);

        System.out.println("Body: " + body);

        JSONObject jsonBody = new JSONObject(body);

        System.out.println("collection:" +  jsonBody.getString("collection"));

        System.out.println("data: " + jsonBody.get("data").toString());

        final String collection = jsonBody.getString("collection");
        final Object data = jsonBody.get("data");

        if(collection == null || collection.isEmpty()) {
            responseJson.put("ack", "0");
            return responseJson.toString();
        }

        JSONObject bQueryRequest = new JSONObject();
        bQueryRequest.put("q","insert");
        bQueryRequest.put("ds", ds);
        bQueryRequest.put("c", collection); //to remove

        JSONArray dataArray = new JSONArray();
        dataArray.put(data);

        JSONObject payloadJson = new JSONObject();
        payloadJson.put("data", dataArray);
        payloadJson.put("c", collection);

        bQueryRequest.put("p", payloadJson);

        System.out.println("Will insert: " + bQueryRequest.toString());

        String responseString = bQueryExecutor.runQuery(bQueryRequest.toString());

        System.out.println("Resonse: " + responseString);

        return responseString;
    }
}
