package com.blobcity.db.config;

import com.blobcity.db.bquery.SQLExecutorBean;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.license.LicenseRules;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DbConfigBean {

    private static final Logger logger = LoggerFactory.getLogger(DbConfigBean.class);

    @Autowired
    private BSqlDataManager dataManager;
    @Autowired @Lazy
    private SQLExecutorBean sqlExecutor;

    public void setConfig(final String key, final String value) throws OperationException {
        JSONObject record = new JSONObject();
        record.put("key", key);
        record.put("value", value);
        sqlExecutor.executePrivileged(".systemdb", "delete from `.systemdb`.`DbConfig` where `key` = '" + key + "'");
        dataManager.insert(".systemdb", "DbConfig", record);
        applyToLicenseRules(key, value);
    }

    public void loadAllConfigs() throws OperationException {
        List<JSONObject> configList = dataManager.selectAll(".systemdb", "DbConfig");
        configList.forEach(config -> {
            try {
                applyToLicenseRules(config.getString("key"), config.getString("value"));
            } catch (OperationException ex) {
                logger.error("Unable to set DB Config: " + config.getString("key") + "=" + config.getString("value"));
            }
        });
    }

    /**
     * Gets the configuration that is set in the {@link LicenseRules} class and not the one stored in the DbConfig
     * table. This function thereby provides information on the configuration currently used by the DB.
     * @param key the config setting key, whos value is to be loaded
     * @return the settings value corresponding to the specified key if the same is found; <code>null</code> otherwise
     */
    public String getConfig(final String key) {
        switch(key.toUpperCase()) {
            case "DATA_CACHING":
                return "" + LicenseRules.DATA_CACHING;
            case "INDEX_CACHING":
                return "" + LicenseRules.INDEX_CACHING;
            case "CACHE_INSERTS":
                return "" + LicenseRules.CACHE_INSERTS;
            case "QUERY_RESULT_CACHING":
                return "" + LicenseRules.QUERY_RESULT_CACHING;
            case "CLUSTERING_AVAILABLE":
                return "" + LicenseRules.CLUSTERING_AVAILABLE;
            case "CLI_QUERY_ANALYSER":
                return "" + LicenseRules.CLI_QUERY_ANALYSER;
            case "FLEXIBLE_SCHEMA":
                return "" + LicenseRules.FLEXIBLE_SCHEMA;
            case "FILE_INTERPRETED_WATCH_SERVICE":
                return "" + LicenseRules.FILE_INTERPRETED_WATCH_SERVICE;
            case "TABLEAU_ENABLED":
                return "" + LicenseRules.TABLEAU_ENABLED;
            case "TABLEAU_AUTO_PUBLIC":
                return "" + LicenseRules.TABLEAU_AUTO_PUBLISH;
            case "TABLEAU_DS_LEVEL_AUTO_PUBLISH":
                return "" + LicenseRules.TABLEAU_DS_LEVEL_AUTO_PUBLISH;
            case "MEMORY_DURABLE_TABLES":
                return "" + LicenseRules.MEMORY_DURABLE_TABLES;
            case "MEMORY_NON_DURABLE_TABLES":
                return "" + LicenseRules.MEMORY_NON_DURABLE_TABLES;
            case "VISUALISATION":
                return "" + LicenseRules.VISUALISATION;
            case "VISUALISATION_ONLY":
                return "" + LicenseRules.VISUALISATION_ONLY;
            case "GEO_REP":
                return "" + LicenseRules.GEO_REP;
            case "STORED_PROCEDURES":
                return "" + LicenseRules.STORED_PROCEDURES;
            case "QPS":
                return "" + LicenseRules.QPS;
            case "QPS_BLOBCITY_SYNC":
                return "" + LicenseRules.QPS_BLOBCITY_SYNC;
        }

        return null;
    }

    private void applyToLicenseRules(final String key, final String value) throws OperationException {
        switch(key.toUpperCase()) {
            case "DATA_CACHING":
                LicenseRules.DATA_CACHING = Boolean.parseBoolean(value);
                break;
            case "INDEX_CACHING":
                LicenseRules.INDEX_CACHING = Boolean.parseBoolean(value);
                break;
            case "CACHE_INSERTS":
                LicenseRules.CACHE_INSERTS = Boolean.parseBoolean(value);
                break;
            case "QUERY_RESULT_CACHING":
                LicenseRules.QUERY_RESULT_CACHING = Boolean.parseBoolean(value);
                break;
            case "CLUSTERING_AVAILABLE":
                LicenseRules.CLUSTERING_AVAILABLE = Boolean.parseBoolean(value);
                break;
            case "CLI_QUERY_ANALYSER":
                LicenseRules.CLI_QUERY_ANALYSER = Boolean.parseBoolean(value);
                break;
            case "FLEXIBLE_SCHEMA":
                LicenseRules.FLEXIBLE_SCHEMA = Boolean.parseBoolean(value);
                break;
            case "FILE_INTERPRETED_WATCH_SERVICE":
                LicenseRules.FILE_INTERPRETED_WATCH_SERVICE = Boolean.parseBoolean(value);
                break;
            case "TABLEAU_ENABLED":
                LicenseRules.TABLEAU_ENABLED = Boolean.parseBoolean(value);
                break;
            case "TABLEAU_AUTO_PUBLIC":
                LicenseRules.TABLEAU_AUTO_PUBLISH = Boolean.parseBoolean(value);
                break;
            case "TABLEAU_DS_LEVEL_AUTO_PUBLISH":
                LicenseRules.TABLEAU_DS_LEVEL_AUTO_PUBLISH = Boolean.parseBoolean(value);
                break;
            case "MEMORY_DURABLE_TABLES":
                LicenseRules.MEMORY_DURABLE_TABLES = Boolean.parseBoolean(value);
                break;
            case "MEMORY_NON_DURABLE_TABLES":
                LicenseRules.MEMORY_NON_DURABLE_TABLES = Boolean.parseBoolean(value);
                break;
            case "VISUALISATION":
                LicenseRules.VISUALISATION = Boolean.parseBoolean(value);
                break;
            case "VISUALISATION_ONLY":
                LicenseRules.VISUALISATION_ONLY = Boolean.parseBoolean(value);
                break;
            case "GEO_REP":
                LicenseRules.GEO_REP = Boolean.parseBoolean(value);
                break;
            case "STORED_PROCEDURES":
                LicenseRules.STORED_PROCEDURES = Boolean.parseBoolean(value);
                break;
            case "QPS":
                LicenseRules.QPS = Boolean.parseBoolean(value);
                break;
            case "QPS_BLOBCITY_SYNC":
                LicenseRules.QPS_BLOBCITY_SYNC = Boolean.parseBoolean(value);
                break;
            default:
                throw new OperationException(ErrorCode.INVALID_CONFIG_KEY);
        }
    }
}
