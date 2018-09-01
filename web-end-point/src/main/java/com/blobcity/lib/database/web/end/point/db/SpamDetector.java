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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

/**
 * REST Web Service
 *
 * @author sanketsarang
 */
@Path("rest/spam-detection")
public class SpamDetector {

    private final Logger logger;
    private final com.blobcity.lib.functions.spam.SpamDetector spamDetector;

    @Autowired
    private SecurityManager securityManager;

    public SpamDetector() {
        this.logger = LoggerFactory.getLogger(SpamDetector.class.getName() + ":" + System.currentTimeMillis());
        ApplicationContext context = BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig");
        this.spamDetector = (com.blobcity.lib.functions.spam.SpamDetector) context.getBean("SpamDetector");
        this.securityManager = (SecurityManager) context.getBean("SecurityManagerBean");
    }

    @POST
    @Produces("application/json")
    public String postRequest(@FormParam(value = "username")
            final String username,
            @FormParam(value = "password")
            final String password,
            @FormParam("msg")
            final String message,
            @FormParam("title")
            final String title) {
        logger.trace("Spam detection on: {} | {}", message, title);
        final long startTime = System.currentTimeMillis();
        final String response;
        if (message == null || message.isEmpty()) {
            response = "{\"ack\":\"0\", \"cause\":\"Invalid request format\"}";
        } else {
            if(!securityManager.verifyCredentials(username, password)) {
                response = "{\"ack\":\"0\", \"cause\":\"Invalid credentials\"}";
            } else {
                final boolean isSpam = spamDetector.isSpam(message, title);
                final JSONObject responseJson = new JSONObject();
                responseJson.put("ack", "1");
                responseJson.put("spam", isSpam);
                response = responseJson.toString();
            }
        }
        final long executionTime = System.currentTimeMillis() - startTime;
        logger.trace("Response to DB query: {}", response);
        logger.trace("Execution time (ms): " + executionTime);
        return response;
    }
}
