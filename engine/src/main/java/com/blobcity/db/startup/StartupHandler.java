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

package com.blobcity.db.startup;

import com.blobcity.db.cluster.nodes.NodeManager;
import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.config.ConfigProperties;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.constants.License;
import com.blobcity.db.global.live.GlobalLiveStore;
import com.blobcity.db.home.HomeReportingBean;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.util.SystemInputUtil;
import com.blobcity.db.versioning.VersionUpgradeFactory;
import com.blobcity.db.versioning.VersionUpgrader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import com.tableausoftware.TableauCredentials;
import com.tableausoftware.beans.TableauConfig;
import org.slf4j.LoggerFactory;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class StartupHandler {

    private static final Logger logger = LoggerFactory.getLogger(StartupHandler.class.getName());
    private static final String RUNNING = "RUNNING";
    private static final String STOPPED = "STOPPED";
    private boolean ready = false;
    @Autowired
    private GlobalLiveStore globalLiveStore;
    @Autowired
    private ConfigBean configBean;
    @Autowired
    private NodeManager nodeManager;
    @Autowired
    private VersionUpgradeFactory versionUpgradeFactory;
    @Autowired
    private HomeReportingBean homeReportingBean;
    
    /* For Singleton initialization purpose on startup */
    @Autowired
    private SchemaStore schemaStore;

    @PostConstruct
    private void boot() {
        
        /* Check node setup else setup node */
        logger.info("Checking if current node is already setup");
        if(!nodeManager.isSetup()) {
            logger.info("No existing setup information found for current node. Setting up a new node of Infinitum.");
            logger.info("USE OF BLOBCITY DB IS SUBJECT TO ACCEPTANCE OF TERMS OF USE LOCATED AT: http://blobcity.com/tou.htm");
            nodeManager.setupNode();
            logger.info("Successfully setup a new node of BlobCity DB. BLOBCITY_DATA is set to " + BSql.BSQL_BASE_FOLDER);
        }
        else{
            logger.info("Node has been already setup. Proceeding");
        }
        
        /* Reports anonymous boot statistics to BlobCity */
//        homeReportingBean.registerWithBlobCity(configBean.getProperty(ConfigProperties.NODE_ID).toString());

        //Check if storage format maps current running version, else upgrade the storage version
        Integer version = (Integer) configBean.getProperty(ConfigProperties.VERSION);
        version = version == null ? 1 : version;
        if (version < License.RELEASE_NUMBER) {
            boolean yes = SystemInputUtil.captureYesNoInput("The database you are running has a higher version than your dsSet. "
                    + "The dsSet needs to be upgraded to the current version for the database to work.  "
                    + "Would you like to update your dsSet version to the latest version? "
                    + "This operation cannot be undone (y/n)?");
            if (yes) {
                performVersionUpgrade();
            } else {
                logger.error("Database cannot be started without the version upgrade. Quitting application.");
                System.exit(0);
            }
        }

        final String status = readStatus();
        switch (status) {
            case STOPPED:
                writeStatus(RUNNING);
                ready = true;
                logger.debug("DB status set to RUNNING");
                break;
            case RUNNING:
                logger.debug("Old DB status was running. Starting with recovery");

                //TODO: Execute any uncompleted tasks that need to be completed before a startup
                writeStatus(RUNNING);
                ready = true;
                logger.debug("DB status set to RUNNING");
                break;
            default:
                throw new RuntimeException("Unable to start server as it appears to have shutdown in an incorrect manner");
        }

        /**
         * Perform boot operations
         */
        /* Start long running tasks and load caches */
        globalLiveStore.init();
    }

    @PreDestroy
    public void shutdown() {
        ready = false;

        //TODO: Check if any operations are running. If running keep status as running only. This will allow them to auto commit on next boot.
        writeStatus(STOPPED);
    }

    public boolean isReady() {
        return ready;
    }

    private String readStatus() {
        Path path = FileSystems.getDefault().getPath(BSql.SERVER_STATUS_FILE);
        try {
            return new String(Files.readAllBytes(path));
        } catch (IOException ex) {
            return "RUNNING"; //this will trigger a failure shutdown restore by default
        }
    }

    private void writeStatus(String status) {
        Path path = FileSystems.getDefault().getPath(BSql.SERVER_STATUS_FILE);
        try {
            Files.write(path, status.getBytes());

        } catch (IOException ex) {
            LoggerFactory.getLogger(StartupHandler.class
                    .getName()).error(null, ex);
            throw new RuntimeException(
                    "Unable to gracefully update server status", ex);
        }
    }

    private void performVersionUpgrade() {
        Integer currentVersion = (Integer) configBean.getProperty(ConfigProperties.VERSION);
        currentVersion = currentVersion == null ? 1 : currentVersion;
        while (currentVersion < License.RELEASE_NUMBER) {
            VersionUpgrader versionUpgrader = versionUpgradeFactory.getVersionUpgrader(++currentVersion);
            versionUpgrader.upgrade();
        // this is incorrect. 
        // if the upgrade is successful, then the version should be changed othervise not
            configBean.setProperty(ConfigProperties.VERSION, currentVersion);
            configBean.updateConfig();
        // this will let us know that database has been upgraded to v4 and 
        // you can create the default tables now
            if(currentVersion == License.RELEASE_NUMBER ){
                configBean.setVersionUpgradedTo4(true);
            }
        }
    }
}
