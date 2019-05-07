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

import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.systemdb.SystemDBService;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Initializes the storage system
 *
 * @author akshaydewan
 * @author sanketsarang
 */
@Component
public class StorageStartup {

    private static final Logger logger = LoggerFactory.getLogger(StorageStartup.class);
    
    @Autowired
    private SystemDBService systemDBService;
    @Autowired  @Lazy
    private ConfigBean configBean;
    
    //This is not a postconstruct method because the db root path will need be passed in the future
    public synchronized void startup() {
        initSystemSchema();
    }

    /**
     * Creates a system schema if it does not exist.
     * A DbRuntimeException is thrown if a problem occurs during this phase
     */
    private void initSystemSchema() {
        logger.info("Data folder location set to: " + BSql.BSQL_BASE_FOLDER);
        File file = new File(BSql.SYSTEM_DB_FOLDER);
        logger.info("Looking for systemdb...");
        if (!file.exists()) {
            logger.info("systemdb not found. Attempting to create and adding default user");
            System.out.println("ccreateSystemDB()");
            createSystemDB();
            System.out.println("createDefaultTables()");
            createDefaultTables();
            System.out.println("addDefaultUser()");
            addDefaultUser();
            checkUsersTableFor_Id();
        } else {
            //TODO else: check version, start upgrade if needed
            logger.info("systemdb found");
            System.out.println("createDefaultTables()");
            createDefaultTables();
        }
        if(configBean.isVersionUpgradedTo4()){
            logger.info("Database version upgrade. Generating new necessary tables");
            upgradeToV4();
        }
    }

    /**
     * create the default tables required for database to work.
     * All these tables are created in .systemdb folder only
     */
    private void createDefaultTables(){
        systemDBService.createUserTable();
        logger.info("user table created successfully");
        
        systemDBService.createNodesTable();
        logger.info("nodes table created successfully");

        systemDBService.createUserGroupsTable();
        logger.info("user table created successfully");

        systemDBService.createSettingsTable();
        logger.info("settings table created successfully");

        systemDBService.createStoredProcedureJarsTable();
        logger.info("SPJars table created successfully");

        systemDBService.createSelectActivityLogTable();
        logger.info("QueryActivity table created successfully");

        systemDBService.createQPSTable();
        logger.info("QPS table created successfully");

        systemDBService.createBillingUsageTable();
        logger.info("BillingUsage table created successfully");

        systemDBService.createApiKeysTable();
        logger.info("ApiKeys table created successfully");
    }

    /**
     * create the systemDB datastore (essential for database)
     */
    private void createSystemDB() {
        systemDBService.createSystemDB();
        logger.info("Created systemdb successfully");
    }

    /**
     * Add default user to `user` table inside systemdb
     */
    private void addDefaultUser() {
        systemDBService.addDefaultUser();
        logger.info("Default user added successfully");
    }

    private void checkUsersTableFor_Id() {

    }

    /**
     * create tables necessary for the upgrade to v4
     * Tables are:
     *  1. user groups
     *  2. folder watch service
     *  3. file watch service
     */
    private void upgradeToV4(){
        logger.error("Skipped creation of tables that were introduced in version 4.");
//        systemDBService.createUserGroupsTable();
//        logger.info("usergroups table created successfully during upgrade");
//
////        systemDBService.createWatchServiceTable();
//        logger.info("watch service tables created successfully");
    }

    
}
