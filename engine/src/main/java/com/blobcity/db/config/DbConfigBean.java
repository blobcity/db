package com.blobcity.db.config;

import com.blobcity.db.bquery.SQLExecutorBean;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.license.LicenseRules;
import com.foundationdb.sql.parser.BooleanConstantNode;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DbConfigBean {

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
            System.out.println(config.toString());
        });
    }

    private void applyToLicenseRules(final String key, final String value) {
        switch(key) {
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
        }
    }
}
