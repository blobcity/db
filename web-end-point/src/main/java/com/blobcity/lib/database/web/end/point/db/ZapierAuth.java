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
import com.blobcity.lib.database.bean.manager.interfaces.security.SecurityManager;
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
@Path("rest/zap-auth")
public class ZapierAuth {

    private final Logger logger;
    private final BQueryExecutor bQueryExecutor;
    private final RequestStore requestStore;
    private final SecurityManager securityManager;

    public ZapierAuth() {
        this.logger = LoggerFactory.getLogger(ZapierAuth.class.getName() + ":" + System.currentTimeMillis());
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
    @GET
    //@POST
    @Produces("application/json")
    public String getJson(@QueryParam("username")
            final String username,
            @QueryParam("password")
            final String password,
            @QueryParam("ds")
            final String ds) {
        logger.trace("Username: {}", username);

//        System.out.println("Username: " + username);
//        System.out.println("Password: " + password);
//        System.out.println("ds: " + ds);

        JSONObject responseJson = new JSONObject();

        if(securityManager.verifyCredentials(username, password)) {
            responseJson.put("ack", "1");
            System.out.println("Auth success");
        } else {
            responseJson.put("ack", "0");
            System.out.println("Auth failure");
        }

        return responseJson.toString();
    }
}
