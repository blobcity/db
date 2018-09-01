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

package com.blobcity.lib.database.web.end.point.bean;

import com.blobcity.lib.database.bean.manager.common.Constants;
import com.blobcity.lib.database.launcher.util.ConfigUtil;
import com.sun.jersey.api.container.ContainerException;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import org.glassfish.grizzly.http.server.HttpServer;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This class acts as the loader for the web endpoint that listens on 0.0.0.0
 * This class acts as the loader for the web endpoint for db.blobcity.com (bound to local IP moving forward)
 *
 * @author javatarz (Karun Japhet)
 * @author sanketsarang
 */
@Component
public class DbEndPointServer {

    private static final Logger logger = LoggerFactory.getLogger(DbEndPointServer.class);
    private HttpServer httpServer = null;

    @PostConstruct
    private void init() {
        final Map<String, List<String>> portConfigData = ConfigUtil.getConfigData(Constants.APP_CONFIG_FILE, "/config/ports/port");
        final int port = Integer.parseInt(portConfigData.get(this.getClass().getName()).get(0));
        final Map<String, List<String>> packageConfigData = ConfigUtil.getConfigData(Constants.APP_CONFIG_FILE, "/config/resources/package");
        final List<String> packageList = packageConfigData.get(this.getClass().getName());
        final Map<String, List<String>> serverAddressData = ConfigUtil.getConfigData(Constants.APP_CONFIG_FILE, "/config/hosts/host");
        final String serverAddress = serverAddressData.get(this.getClass().getName()).get(0);

        try {
            final ResourceConfig resourceConfig = new PackagesResourceConfig(packageList.toArray(new String[packageList.size()]));
//            final String serverAddress = "http://0.0.0.0"; //TODO: This should be externally configurable
            logger.debug("Attempting to start the server at address {} with port {}", serverAddress, port);
            httpServer = GrizzlyServerFactory.createHttpServer(UriBuilder.fromUri(serverAddress).port(port).build(), resourceConfig);
            httpServer.getListeners().forEach(listener -> listener.setMaxPostSize(Integer.MAX_VALUE));
            httpServer.getListeners().forEach(listener -> listener.setMaxHttpHeaderSize(Integer.MAX_VALUE));
            logger.info("HTTP server has started. Web End Point for the database is ready."); // this is obviously non complete code
        } catch (ContainerException ex) {
            logger.error("Web End Point specified by {} couldn't be started because no root resource classes could be found in the listed packages ({}). Confirm if the packages have been refactored.", this.getClass(), packageList);
        } catch (IOException | IllegalArgumentException | NullPointerException ex) {
            logger.error("Exception occurred while attempting to start the server. Starting the server failed.", ex);
        }
    }

    @PreDestroy
    private void destroy() {
        if (httpServer != null) {
            logger.info("HTTP server is being stopped");
            httpServer.stop();
        }
    }
}
