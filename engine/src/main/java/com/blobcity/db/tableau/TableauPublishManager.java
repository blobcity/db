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

package com.blobcity.db.tableau;

import com.blobcity.db.bquery.InternalQueryBean;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.features.FeatureRules;
import com.blobcity.lib.query.CollectionStorageType;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * @author sanketsarang
 */
@Component
public class TableauPublishManager {

    @Autowired
    private InternalQueryBean internalQueryBean;
    @Autowired
    private TableauPublishStore tableauPublishStore;

    public void setAutoPublishOn(final String datastore) throws OperationException {
        setAutoPublish(datastore, true);
        tableauPublishStore.setAutoPublishOn(datastore);
    }

    public void setAutoPublishOff(final String datastore) throws OperationException {
        setAutoPublish(datastore, false);
        tableauPublishStore.setAutoPublishOff(datastore);
    }

    public void setAutoPublishOn(final String datastore, final String collection) throws OperationException {
        setAutoPublish(datastore, collection, true);
        tableauPublishStore.setAutoPublishOn(datastore, collection);
    }

    public void setAutoPublishOff(final String datastore, final String collection) throws OperationException {
        setAutoPublish(datastore, collection, false);
        tableauPublishStore.setAutoPublishOff(datastore, collection);
    }

    public List<JSONObject> getAllAutoPublishSettings() throws OperationException {
        if(!FeatureRules.TABLEAU_AUTO_PUBLISH) {
            return Collections.emptyList();
        }

        List<JSONObject> records;
        try {
            records = internalQueryBean.select(BSql.SYSTEM_DB, "select * from `"
                    + BSql.SYSTEM_DB + "`.`" + BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE + "`");
        } catch (OperationException ex) {
            return Collections.emptyList();
        }

        return records;
    }

    private void setAutoPublish(final String datastore, final Boolean on) throws OperationException {
        if(!FeatureRules.TABLEAU_AUTO_PUBLISH) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Current version does not support " +
                    "tableau-auto-publish feature");
        }

        if(!FeatureRules.TABLEAU_DS_LEVEL_AUTO_PUBLISH) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Current version does not support " +
                    "tableau-auto-publish feature at datastore level. Attempt setting for individual collections");
        }

        List<JSONObject> records;
        try {
            records = internalQueryBean.select(BSql.SYSTEM_DB, "select * from `"
                    + BSql.SYSTEM_DB + "`.`" + BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE
                    + "` WHERE `ds`='" + datastore + "'");
        } catch (OperationException ex) {
            records = Collections.EMPTY_LIST;
        }

        if(records.isEmpty()) {
            JSONObject jsonRecord = new JSONObject();
            jsonRecord.put("ds", datastore);
            jsonRecord.put("auto", on);
            if(!internalQueryBean.collectionExists(BSql.SYSTEM_DB, BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE)) {
                internalQueryBean.createCollection(BSql.SYSTEM_DB, BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE, CollectionStorageType.ON_DISK);
            }
            internalQueryBean.insert(BSql.SYSTEM_DB, BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE, jsonRecord);
        } else {
            internalQueryBean.update(datastore, "update `" + BSql.SYSTEM_DB + "`.`"
                    + BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE
                    + "` SET `auto`=" + on + " where `ds`='" + datastore + "'");
        }
    }

    private void setAutoPublish(final String datastore, final String collection, final Boolean on) throws OperationException {
        if(!FeatureRules.TABLEAU_AUTO_PUBLISH) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Current version does not support " +
                    "tableau-auto-publish feature");
        }

        List<JSONObject> records;
        try {
            records = internalQueryBean.select(BSql.SYSTEM_DB, "select * from `"
                    + BSql.SYSTEM_DB + "`.`" + BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE
                    + "` WHERE `ds`='" + datastore + "' AND `collection`='" + collection + "'");
        } catch (OperationException ex) {
            records = Collections.EMPTY_LIST;
        }

        if(records.isEmpty()) {
            JSONObject jsonRecord = new JSONObject();
            jsonRecord.put("ds", datastore);
            jsonRecord.put("collection", collection);
            jsonRecord.put("auto", on);
            if(!internalQueryBean.collectionExists(BSql.SYSTEM_DB, BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE)) {
                internalQueryBean.createCollection(BSql.SYSTEM_DB, BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE, CollectionStorageType.ON_DISK);
            }
            internalQueryBean.insert(BSql.SYSTEM_DB, BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE, jsonRecord);
        } else {
            internalQueryBean.update(BSql.SYSTEM_DB, "update `" + BSql.SYSTEM_DB + "`.`"
                    + BSql.TABLEAU_AUTO_PUBLISH_CONFIG_TABLE
                    + "` SET `auto`=" + on + " where `ds`='" + datastore + "' AND `collection`='" + collection + "'");
        }

        if(on) {
            tableauPublishStore.setAutoPublishOn(datastore, collection);
        } else {
            tableauPublishStore.setAutoPublishOff(datastore, collection);
        }
    }
}
