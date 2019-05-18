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

package com.blobcity.db.systemdb;

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDatastoreManager;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.security.User;
import com.blobcity.db.bquery.SQLExecutorBean;
import com.blobcity.db.exceptions.DbRuntimeException;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.security.UserManager;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.UUID;

/**
 *
 * Services for accessing SystemDB. This is a 'dumb' service used only for data access
 *
 * @author akshaydewan
 * @author sanketsarang
 */
@Component
public class SystemDBService {

    private static final Logger logger = LoggerFactory.getLogger(SystemDBService.class);

    @Autowired
    private SQLExecutorBean sqlExecutor;
    @Autowired @Lazy
    private UserManager userManager;
    @Autowired @Lazy
    private BSqlCollectionManager collectionManager;

    
    /**
     * Creates a SystemDB schema from scratch
     */
    public synchronized void createSystemDB() {
        String query = "CREATE SCHEMA `.systemdb`";
        String output = sqlExecutor.executePrivileged(".systemdb", query);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create systemdb. Will crash.");
            throw new DbRuntimeException("Failed to create systemdb");
        }
    }

    /**
     * Creates a User table
     */
    public synchronized void createUserTable() {
        if(collectionManager.exists(".systemdb", "user")) {
            return;
        }

        String sql = "CREATE TABLE `user` ("
                + "username    VARCHAR(255) NOT NULL,"
                + "password    VARCHAR(255) NOT NULL,"
                + "defaultRole VARCHAR(255) DEFAULT NULL"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create user table with error [" + json.getString("code") + ": " 
                    + json.getString("cause") + "]. Will crash.");
            throw new DbRuntimeException("Failed to create user table");
        }
    }
    
    /**
     * Creates a Nodes table
     */
    public synchronized void createNodesTable() {
        if(collectionManager.exists(".systemdb", "nodes")) {
            return;
        }

        String sql = "CREATE TABLE `nodes` ("
                + "nodeId   VARCHAR(255) NOT NULL,"
                + "ip       VARCHAR(255),"
                + "port     INT,"
                + "license  VARCHAR(255)"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create nodes table. Clustering will crash.");
            throw new DbRuntimeException("Failed to create nodes table");
        }
    }
    
    /**
     * Creates a User Groups Table
     */
    public synchronized void createUserGroupsTable(){
        if(collectionManager.exists(".systemdb", "usergroups")) {
            return;
        }

        String sql = "CREATE TABLE `usergroups` ("
                + "name     VARCHAR(255) NOT NULL,"
                + "users    VARCHAR(255),"
                + "owner    VARCHAR(255),"
                + "createdAt   VARCHAR(255)"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create user groups table");
            throw new DbRuntimeException("Failed to create usergroups table");
        }
    }

    /**
     * Creates a Watch Service Table
     * TODO: define the proper schema here
     */
    public synchronized void createWatchServiceTable(){
        if(collectionManager.exists(".systemdb", "watchservice")) {
            return;
        }

        String sql = "CREATE TABLE `watchservice` ("
                + "path     VARCHAR(255),"
                + "file-list    VARCHAR(255)"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create watchservice table");
            throw new DbRuntimeException("Failed to create watchservice table");
        }
    }

    /**ot
     * Creates a settings table
     * TODO: define the proper schema here
     */
    public synchronized void createSettingsTable(){
        if(collectionManager.exists(".systemdb", "settings")) {
            return;
        }

        String sql = "CREATE TABLE `settings` ("
                + "value     VARCHAR(255)"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create settings table");
            throw new DbRuntimeException("Failed to create settings table");
        }
    }

    public synchronized void createSelectActivityLogTable(){
        if(collectionManager.exists(".systemdb", "SelectActivityLog")) {
            return;
        }

        String sql = "CREATE TABLE `SelectActivityLog` ("
                + "`ds` VARCHAR(255) NOT NULL,"
                + "`rows` BIGINT"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create SelectActivityLog table");
            throw new DbRuntimeException("Failed to create SelectActivityLog table");
        }
    }

    public synchronized void createQPSTable(){
        if(collectionManager.exists(".systemdb", "QPS")) {
            return;
        }

        String sql = "CREATE TABLE `QPS` ("
                + "`ds` VARCHAR(255) NOT NULL,"
                + "`c` VARCHAR(255),"
                + "`sql` VARCHAR(255),"
                + "`time` BIGINT,"
                + "`rows` BIGINT,"
                + "`timestamp` BIGINT"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create SelectActivityLog table");
            throw new DbRuntimeException("Failed to create SelectActivityLog table");
        }
    }

    public synchronized void createBillingUsageTable() {
        if(collectionManager.exists(".systemdb", "BillingUsage")) {
            return;
        }

        String sql = "CREATE TABLE `BillingUsage` ("
                + "`ds`     VARCHAR(255) NOT NULL,"
                + "`my`     VARCHAR(255),"
                + "`rows`     BIGINT"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create BillingUsage table");
            throw new DbRuntimeException("Failed to create BillingUsage table");
        }
    }

    public synchronized void createApiKeysTable() {
        if(collectionManager.exists(".systemdb", "ApiKeys")) {
            return;
        }

        String sql = "CREATE TABLE `ApiKeys` ("
                + "`key`     VARCHAR(255) NOT NULL,"
                + "`ds`     VARCHAR(255)"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create ApiKeys table");
            throw new DbRuntimeException("Failed to create ApiKeys table");
        }
    }

    /**
     * Adds a default user with the username and password as 'root'
     */
    public synchronized void addDefaultUser() {
        User user = new User();
        user.setUsername("root");
        final String password = randomPassword();
        user.setPassword(password);
        user.setDefaultRole("");
        try {
            userManager.addUser(user);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(BSql.INITIAL_CREDENTIALS_FILE))));
            writer.write(password);
            writer.newLine();
            writer.close();
        } catch (OperationException ex) {
            logger.error("Failed to add default user", ex);
            throw new DbRuntimeException(ex);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void createSystemPermissionsTable() {
        //TODO: Implement this
    }

    public synchronized void createDataPermissionsTable() {
        //TODO: Implement this
    }

    /**
     * Creates the SPJars table. The table is used to store the names of the jars that were last loaded as DB modules.
     * The information is saved on a per datastores basis. It is at max one jar per datastore
     */
    public synchronized void createStoredProcedureJarsTable(){
        if(collectionManager.exists(".systemdb", "SPJars")) {
            return;
        }

        String sql = "CREATE TABLE `SPJars` ("
                + "ds     VARCHAR(255),"
                + "jar     VARCHAR(255)"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create Stored Procedures Jar's table");
            throw new DbRuntimeException("Failed to create Stored Procedures Jar's table");
        }
    }

    public synchronized void createWatchServiceFileTrackerTable() {
        if(collectionManager.exists(".systemdb", "WSFileTracker")) {
            return;
        }

        String sql = "CREATE TABLE `WSFileTracker` ("
                + "ds     VARCHAR(255),"
                + "folder     VARCHAR(255),"
                + "file     VARCHAR(255),"
                + "lineNo     VARCHAR(255),"
                + "md5     VARCHAR(255),"
                + "status   VARCHAR(255)"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create Watch Service File Tracker table");
            throw new DbRuntimeException("Failed to create Watch Service File Tracker table");
        }
    }

    public synchronized void createWatchServiceTrackerTable() {
        if(collectionManager.exists(".systemdb", "WSTracker")) {
            return;
        }

        String sql = "CREATE TABLE `WSTracker` ("
                + "ds     VARCHAR(255),"
                + "folder     VARCHAR(255),"
                + "file     VARCHAR(255),"
                + "length   LONG,"
                + "progress  DOUBLE,"
                + "status   VARCHAR(255)"
                + ")";
        String output = sqlExecutor.executePrivileged(".systemdb", sql);
        JSONObject json = new JSONObject(output);
        if (!json.getString("ack").equals("1")) {
            logger.error("Failed to create Watch Service Tracker table");
            throw new DbRuntimeException("Failed to create Watch Service Tracker table");
        }
    }

    private String randomPassword() {
        String uuid = UUID.randomUUID().toString();
        uuid = uuid.replaceAll("-", "");
        return uuid.substring(4, 14);
    }
}
