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

package com.blobcity.lib.database.launcher.main;

import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.ftp.FtpServerManager;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.startup.StorageStartup;
import com.blobcity.lib.database.bean.manager.common.Constants;
import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.database.launcher.util.ConfigUtil;
import com.blobcity.pom.database.engine.factory.EngineBeanConfig;
import java.util.List;
import java.util.Map;

import com.tableausoftware.TableauCredentials;
import com.tableausoftware.beans.TableauConfig;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Main class to launch the database instance up by loading Bean Factories in child projects. Each bean factory will
 * load it's startup beans to start processes. Most run time singleton beans will be instantiated lazily.
 *
 * If you need to addApplicationContext any child project, you can do so by adding an entry to
 * {@code /src/main/resources/launcher-config.xml}
 *
 * @author javatarz (Karun Japhet)
 */
public final class Startup {

    private static final Logger logger = LoggerFactory.getLogger(Startup.class);

    /**
     * Private constructor to prevent instances from being created externally
     */
    private Startup() {
        // do nothing
    }

    /**
     * Method starts the database package and it's libraries by loading all their bean factories each of which is
     * responsible to load it's own code henceforth.
     *
     * List of bean factories is maintained under {@code /src/main/resources/launcher-config.xml}.
     *
     * TODO: 1) I had a dream.. a dream that some day main functions will parse CLI arguments.
     *
     * @param args who cares.. for now..
     */
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        logger.info("Starting BlobCity DB\"");

        // Pre-startup tasks below
        logger.debug("Configuring JUL Loggers");

        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        System.setProperty("jna.debug_load", "true");
        System.setProperty("jna.debug_load.jna", "true");

//        System.setProperty("java.library.path", "/lib");
        System.setProperty("jna.library.path", "/lib64");

        // Startup tasks below
        logger.debug("Performing database launch tasks");
        try {
            loadBeanConfigs(); // This task launches the database

            // Any other future tasks can go here as method class
            initEngine();
        } catch (ClassNotFoundException ex) {
            logger.error("Launching BlobCity DB has failed!", ex);
        }
        logger.info("BlobCity DB successfully started in " + (System.currentTimeMillis() - startTime) / 1000 + " seconds");

        logger.info("Setting log level to ERROR. Change by using set-log-level {log_level} CLI command");

        LogManager.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
        // it is assumed that startup failures are managed and appropriate logs are generated
    }

    /**
     * Calls initialization tasks for the engine. If anything goes wrong, a runtime exception will be thrown and the
     * database will not start
     */
    private static void initEngine() {
        ApplicationContext context = BeanConfigFactory.getConfigBean(EngineBeanConfig.class.getName());
        StorageStartup storageStartup = context.getBean(StorageStartup.class);
        storageStartup.startup();
        FtpServerManager ftpServerManager = context.getBean(FtpServerManager.class);
        try {
            ftpServerManager.start();
        } catch (OperationException e) {
            logger.error("FTP service will be disabled", e);
        }

        /* Init Tableau */
//        TableauConfig tableauConfig = context.getBean(TableauConfig.class);
//        tableauConfig.init(PathUtil.tableauConfigFile(), PathUtil.tableauSchemaFile(), TableauCredentials.getDefault());
    }

    /**
     * This method loads all bean configurations in the project helping the application initialize.
     *
     * @throws ClassNotFoundException if the bean factory class is not found. If this exception occurs, double check the
     * value in the configuration file versus the fully qualified class path of the Bean Factory class to be loaded and
     * ensure that the Launcher project can see this class (it should be added as a dependency).
     */
    private static void loadBeanConfigs() throws ClassNotFoundException {
        final Map<String, List<String>> configMap = ConfigUtil.getConfigData(Constants.APP_CONFIG_FILE, "/config/factories/name");

        for (final Map.Entry<String, List<String>> entry : configMap.entrySet()) {
            final List<String> configValueList = entry.getValue();
            for (final String configValue : configValueList) {
                logger.debug("Attempting to load bean factory \"{}\"", configValueList);

                final ApplicationContext context = new AnnotationConfigApplicationContext(Class.forName(configValue));
                BeanConfigFactory.addApplicationContext(configValue, context);
                logger.info("Loaded Application Configuration \"{}\"", (!"".equals(context.getApplicationName()) ? context.getApplicationName() : configValue));
            }
        }
    }
}
