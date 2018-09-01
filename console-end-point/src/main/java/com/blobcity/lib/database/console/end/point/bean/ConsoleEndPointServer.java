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

package com.blobcity.lib.database.console.end.point.bean;

import com.blobcity.lib.database.console.end.point.service.ConsoleEndPointServerSocket;
import com.blobcity.lib.database.bean.manager.common.Constants;
import com.blobcity.lib.database.launcher.util.ConfigUtil;
import com.sun.jersey.api.container.ContainerException;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import org.glassfish.grizzly.http.server.HttpServer;

import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This class acts as the loader for the console end point for communicating with the database
 *
 * @author javatarz (Karun Japhet)
 * @author sanketsarang
 */
@Component
public class ConsoleEndPointServer {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleEndPointServer.class);
    private HttpServer httpServer = null;

    @PostConstruct
    private void init() {
        final Map<String, List<String>> portConfigData = ConfigUtil.getConfigData(Constants.APP_CONFIG_FILE, "/config/ports/port");
        final int port = Integer.parseInt(portConfigData.get(this.getClass().getName()).get(0));
        final Map<String, List<String>> packageConfigData = ConfigUtil.getConfigData(Constants.APP_CONFIG_FILE, "/config/resources/package");
        final List<String> packageList = packageConfigData.get(this.getClass().getName());

        try {
            final ResourceConfig resourceConfig = new PackagesResourceConfig(packageList.toArray(new String[packageList.size()]));
            final String serverAddress = "localhost";

            logger.debug("Attempting to start CLI listener at address {} with port {}", serverAddress, port);
            new Thread(new ConsoleEndPointServerSocket(port)).start();
            logger.info("CLI listener has started. CLI end point for database is ready."); // this is obviously non complete code
        } catch (ContainerException ex) {
            logger.error("CLI specified by {} couldn't be started because no root resource classes could be found in the listed packages ({}). Confirm if the packages have been refactored.", this.getClass(), packageList);
        }
    }

    @PreDestroy
    private void destroy() {
        if (httpServer != null) {
            logger.info("CLI listener is being stopped");
            httpServer.stop();
        }
    }
}
