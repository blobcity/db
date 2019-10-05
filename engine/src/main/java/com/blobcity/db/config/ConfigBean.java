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

package com.blobcity.db.config;

import com.blobcity.db.constants.BSql;
import com.blobcity.db.locks.ReadWriteSemaphore;
//import com.blobcity.security.consume.LicenseManager;
//import com.blobcity.security.exception.InternalException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

//import com.blobcity.license.License;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * TODO: The ConfigBean should read nothing from the config file except for node-id and license. All other data needs to
 * be moved into the system_db folder.
 *
 * Stores configuration parameters loaded from config.json configuration file. Also provides methods to write to the
 * config.json file.
 *
 * @author sanketsarang
 */
@Component
public class ConfigBean {

    private static final int CONCURRENT_ACCESS_PERMITES = 128;
    public Map<String, Object> configMap = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(ConfigBean.class.getName());
    private final ReadWriteSemaphore semaphore = new ReadWriteSemaphore(CONCURRENT_ACCESS_PERMITES);

    //NOTETOSELF:
    // think of any other way to let storage system know that database has been upgraded to v4
    private static boolean versionUpgradedTo4 = false;

    @PostConstruct
    private void init() {
        try {
            logger.info("Machine IP address: {}", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException ex) {
            logger.error("The local host name could not be resolved into an address.", ex);
        }

        if (configurationExists()) {
            loadConfig();
        }

        /* Get the node id of this node from the LicenseManager */
//        try {
//            configMap.put(ConfigProperties.NODE_ID, LicenseManager.getServerId());
//        } catch (TimeLimitExceededException | InternalException ex) {
//            logger.error("Failed to detect/generate server id. The database may be unusable", ex);
//        }

        /* Temp code to add a random nodeId to all nodes */
        configMap.put(ConfigProperties.NODE_ID, UUID.randomUUID().toString());
    }

    public Object getProperty(String key) {
        return configMap.get(key);
    }

    public String getStringProperty(String key) {
        Object value = configMap.get(key);

        if (value != null) {
            return value.toString();
        }

        return null;
    }

    public void setProperty(String key, Object value) {
        configMap.put(key, value);
    }

    public boolean contains(String key) {
        return configMap.containsKey(key);
    }

    public void updateConfig() {
        JSONObject jsonObject = new JSONObject(configMap);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(BSql.CONFIF_FILE))) {
            writer.write(jsonObject.toString());
        } catch (IOException ex) {
            logger.error("I/O exception occurred during the writing of the config", ex);
        }
    }

    public void acquireExclusiveAccess() throws InterruptedException {
        semaphore.acquireWriteLock();
    }

    public void releaseExclusiveAccess() throws InterruptedException {
        semaphore.releaseWriteLock();
    }

    private void loadConfig() {
        JSONObject jsonObject;
        configMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(BSql.CONFIF_FILE))) {
            jsonObject = new JSONObject(reader.readLine());
            Iterator<String> iterator = jsonObject.keys();
            while (iterator.hasNext()) {
                String key = iterator.next();
                try {
                    configMap.put(key, jsonObject.get(key));
                } catch (JSONException ex) {
                    logger.error("Error while reading keys from the config file", ex);
                }
            }
        } catch (IOException ex) {
            logger.error("Exception occurred during reading the config file", ex);
        } catch (JSONException ex) {
            logger.error("Unable to parse config file contents as JSON", ex);
        }

//            configMap.put(ConfigProperties.NODE_ID, License.getNodeId());
            configMap.put(ConfigProperties.NODE_ID, "default"); //temp code until remove of licensing module
    }

    private boolean configurationExists() {
        File file = new File(BSql.CONFIF_FILE);
        return file.exists();
    }

    public void createDefaultConfig(int storageVersion) {
        configMap = new HashMap<>();

        /* Auto detect the broadcast address for clsutering */
        try {
            String ip = InetAddress.getLocalHost().getHostAddress();
            ip = ip.substring(0, ip.lastIndexOf(".") + 1) + "255";
            configMap.put(ConfigProperties.CLUSTER_BROADCAST_IP, ip);
            configMap.put(ConfigProperties.VERSION, storageVersion);
//            configMap.put(ConfigProperties.NODE_ID, LicenseManager.getServerId());
            configMap.put(ConfigProperties.NODE_ID, UUID.randomUUID().toString());
        } catch (UnknownHostException ex) {
            logger.warn("Automatic configuration could not detect broadcast address for clustering. This will have to be configured manaully for clustering to be enabled.");
        }
//        catch (TimeLimitExceededException ex) {
//            java.util.logging.Logger.getLogger(ConfigBean.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        catch (InternalException ex) {
//            java.util.logging.Logger.getLogger(ConfigBean.class.getName()).log(Level.SEVERE, null, ex);
//        }

        updateConfig();
        loadConfig();
    }

//    /**
//     * This function smartly detects the version number of the database that the storage is currently in if data already
//     * isPresent on the node, but a configuration file specifying the current version number is not found.
//     *
//     * This method is not full proof and the logic needs to be checked with every new release version.
//     */
//    private int getStorageVersion() {
//        
//        if (Files.isPresent(FileSystems.getDefault().getPath(BSql.OLD_BSQL_BASE_FOLDER))) {
//            return 1;
//        }
//
//        if (!Files.isPresent(FileSystems.getDefault().getPath(BSql.SYSTEM_DB_FOLDER))) {
//            return 2;
//        }
//
//        return 3;
//    }

    public boolean isVersionUpgradedTo4() {
        return versionUpgradedTo4;
    }

    public void setVersionUpgradedTo4(boolean versionUpgradedTo4) {
        this.versionUpgradedTo4 = versionUpgradedTo4;
    }
}
