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

package com.blobcity.db.license;

import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import javax.annotation.PostConstruct;
import com.blobcity.license.Attribute;
import com.blobcity.license.LicenseException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 *
 * @author sanketsarang
 */
@Component
@Deprecated
public class LicenseBean {

    private static final Logger logger = LoggerFactory.getLogger(LicenseBean.class);
    private static final long MAX_RECHECK_INTERVAL = 2 * 60 * 1000; //2 min in milli seconds
    private boolean active = false;
    private long dbSize = -1; //the amount of data that the database can have in bytes
    private long validUntil = -1;
    public long lastChecked = -1;
    @Autowired
    private ConfigBean configBean;

    @PostConstruct
    public void init() {
        validateLicense();
    }

    @Scheduled(fixedRate = 60000)
    public void timeout() {
        validateLicense();
    }

    public boolean isActive() {
        final long timeCheckDifference = (System.currentTimeMillis() - lastChecked);

        logger.trace("LicenseBean.isActive(): Time check: {} (timeCheckDifference: {}, MAX_RECHECK_INTERVAL: {}), active: {}", timeCheckDifference < MAX_RECHECK_INTERVAL, timeCheckDifference, MAX_RECHECK_INTERVAL, active);
        return timeCheckDifference < MAX_RECHECK_INTERVAL && active;
    }

    public long getDbSize() {
        return LicenseRules.DATA_LIMIT;
    }

    public long getValidUntil() {
        return LicenseRules.EXPIRES;
    }

    @Deprecated
    public void applyLicense(final String license) throws OperationException {
//        validateLicense(license);
//        configBean.setProperty(ConfigProperties.LICENSE, license);
//        configBean.updateConfig();
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "apply-license function is no longer supported. Place the license file in the base data folder on the node");
    }

    public void revokeLicense() throws OperationException {
//        configBean.setProperty(ConfigProperties.LICENSE, "");
//        configBean.updateConfig();
//        active = false;
//        dbSize = -1;
//        validUntil = -1;
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "revoke-license function is no longer supported. Remove the license file from the node to revoke license");
    }

    private void validateLicense() {
        logger.trace("Validating license key");
        try {
            logger.trace("Public key file: " + BSql.LICENSE_PUBLIC_KEY_FILE);
            final JSONObject licenseJson = com.blobcity.license.License.getLicenseDetails(BSql.LICENSE_PUBLIC_KEY_FILE, BSql.NODE_LICENSE_FILE);
            final String product = licenseJson.getString(Attribute.PRODUCT.getAttribute());
            final int version = licenseJson.getInt(Attribute.VERSION.getAttribute());
            final long expires = licenseJson.getLong(Attribute.EXPIRES.getAttribute());

            final JSONObject featuresJson = licenseJson.getJSONObject(Attribute.FEATURES.getAttribute());
            LicenseRules.DATA_CACHING = featuresJson.getBoolean(Attribute.DATA_CACHING.getAttribute());
            LicenseRules.INDEX_CACHING = featuresJson.getBoolean(Attribute.INDEX_CACHING.getAttribute());
            LicenseRules.CACHE_INSERTS = featuresJson.getBoolean(Attribute.CACHE_INSERTS.getAttribute());
            LicenseRules.QUERY_RESULT_CACHING = featuresJson.getBoolean(Attribute.QUERY_RESULT_CACHING.getAttribute());
            LicenseRules.DATA_LIMIT = featuresJson.getLong(Attribute.DATA_LIMIT.getAttribute());
            LicenseRules.MEMORY_DURABLE_TABLES = featuresJson.getBoolean(Attribute.IN_MEMORY.getAttribute());
            LicenseRules.MEMORY_NON_DURABLE_TABLES = featuresJson.getBoolean(Attribute.IN_MEMORY_ND.getAttribute());
            LicenseRules.VISUALISATION_ONLY = featuresJson.getBoolean(Attribute.VIS_ONLY.getAttribute());
            LicenseRules.VISUALISATION = featuresJson.getBoolean(Attribute.VISUALISATION.getAttribute());
            LicenseRules.CLUSTERING_AVAILABLE = featuresJson.getBoolean(Attribute.CLUSTERING.getAttribute());
            LicenseRules.GEO_REP = featuresJson.getBoolean(Attribute.GEO_REP.getAttribute());
            LicenseRules.STORED_PROCEDURES = featuresJson.getBoolean(Attribute.STORED_PROCEDURES.getAttribute());
            LicenseRules.BYPASS_ROOT_ONLY = featuresJson.getBoolean(Attribute.BYPASS_ROOT_ONLY.getAttribute());
            LicenseRules.ALLOW_LIST_DS = featuresJson.getBoolean(Attribute.LIST_DS.getAttribute());
            logger.trace("License is applied");
        } catch (LicenseException ex) {
            //default to free edition features
            logger.trace("No license file found at " + BSql.NODE_LICENSE_FILE + ". Product will operate with features of free edition.");
            setLicenseToFreeEdition();
        } catch (Exception ex) {
            //temp catch block until licensing module is removed
        }
        lastChecked = System.currentTimeMillis();
    }

    private void setLicenseToFreeEdition() {
        LicenseRules.DATA_CACHING = false;
        LicenseRules.INDEX_CACHING = false;
        LicenseRules.CACHE_INSERTS = false;
        LicenseRules.CLUSTERING_AVAILABLE = false;
        LicenseRules.CLI_QUERY_ANALYSER = false;
        LicenseRules.FLEXIBLE_SCHEMA = true;
        LicenseRules.FILE_INTERPRETED_WATCH_SERVICE = false;
        LicenseRules.TABLEAU_AUTO_PUBLISH = false;
        LicenseRules.TABLEAU_DS_LEVEL_AUTO_PUBLISH = false;
        LicenseRules.MEMORY_DURABLE_TABLES = false;
        LicenseRules.MEMORY_NON_DURABLE_TABLES = false;
        LicenseRules.BYPASS_ROOT_ONLY = true;
        LicenseRules.ALLOW_LIST_DS = true;
        LicenseRules.VISUALISATION = false;
        LicenseRules.VISUALISATION_ONLY = false;
        LicenseRules.DATA_LIMIT = -1; // -1 means unlimited data. Limit is otherwise specified in GB's and applies to whole cluster this node is a part of
        LicenseRules.EXPIRES = -1;
        LicenseRules.GEO_REP = false;
        LicenseRules.STORED_PROCEDURES = false;
    }

    /**
     * This feature must be used only when releasing Docker Enterprise edition containers for selling on the Docker Store
     */
    private void setLicenseToEnterpriseEdition() {
        LicenseRules.DATA_CACHING = true;
        LicenseRules.INDEX_CACHING = true;
        LicenseRules.CACHE_INSERTS = true;
        LicenseRules.CLUSTERING_AVAILABLE = true;
        LicenseRules.CLI_QUERY_ANALYSER = true;
        LicenseRules.FLEXIBLE_SCHEMA = true;
        LicenseRules.FILE_INTERPRETED_WATCH_SERVICE = true;
        LicenseRules.TABLEAU_AUTO_PUBLISH = true;
        LicenseRules.TABLEAU_DS_LEVEL_AUTO_PUBLISH = true;
        LicenseRules.MEMORY_DURABLE_TABLES = true;
        LicenseRules.MEMORY_NON_DURABLE_TABLES = true;
        LicenseRules.BYPASS_ROOT_ONLY = true;
        LicenseRules.ALLOW_LIST_DS = true;
        LicenseRules.VISUALISATION = true;
        LicenseRules.VISUALISATION_ONLY = true;
        LicenseRules.DATA_LIMIT = -1; // -1 means unlimited data. Limit is otherwise specified in GB's and applies to whole cluster this node is a part of
        LicenseRules.EXPIRES = -1; //means never expires
        LicenseRules.GEO_REP = true;
        LicenseRules.STORED_PROCEDURES = true;
    }

//    private void validateLicense(final String license) throws OperationException {
//
//        //TODO: Uncomment code after the original code of the key-consumer library is found
//
////        try {
////            if (!LicenseManager.isKeyValid(license, LicenseManager.getServerId(), License.PRODUCT_TYPE, License.VERSION, License.SALT)) {
////                throw new OperationException(ErrorCode.INVALID_LICENSE);
////            }
////
////            final String keyData = LicenseManager.getKeyData(license, LicenseManager.getServerId(), License.PRODUCT_TYPE, License.VERSION, License.SALT);
////            JSONObject jsonKeyData = new JSONObject(keyData);
////            validUntil = jsonKeyData.getLong("validity");
////
////            /* get meta information present in the license. Usually specific to license type */
////            JSONObject jsonData = new JSONObject(jsonKeyData.getString("data"));
////            dbSize = SizeFormat.toBytes(jsonData.getString("db-size"));
////
////            /* Check that current date is within the validity date */
////            if (System.currentTimeMillis() > validUntil) {
////                throw new OperationException(ErrorCode.LICENSE_EXPIRED);
////            }
////
////            active = true;
////        } catch (TimeLimitExceededException ex) {
////            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "License key could not be validated. Please try after sometime.", ex);
////        } catch (InternalException | JSONException ex) {
////            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, ex);
////        }
//    }
}
