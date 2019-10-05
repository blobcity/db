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

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.features.FeatureRules;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.util.*;

/**
 * @author sanketsarang
 */
@Component
public class TableauPublishStore {

    private static final Logger logger = LoggerFactory.getLogger(TableauPublishStore.class.getName());

    private final Map<String, Boolean> dsMap = new HashMap<>();
    private final Map<String, Map<String, Boolean>> collectionMap = new HashMap<>();

//    private final Map<String, Map<String, Long>> lastPublished = new HashMap<>();
//    private final Map<String, Map<String, Boolean>> currentlyPublishing = new HashMap<>();
    private final Set<DsAndC> publishQueue = new HashSet<>();
    private final long PUBLISH_INTERVAL = 60000;

    private final Boolean defaultValue = true;

    @Autowired
    private BSqlCollectionManager collectionManager;
    @Autowired
    private TableauPublishManager tableauPublishManager;
    @Autowired
    private TableauTdeManager tableauTdeManager;

    @PostConstruct
    public void loadSettings() {
        System.out.println("Load settings called");
        List<JSONObject> records = null;
        try {
            records = tableauPublishManager.getAllAutoPublishSettings();
        } catch (OperationException e) {
            logger.warn("Unable to load Tableau Auto-Publish configuration. Auto-publish may be disabled");
            return;
        }

        for(JSONObject record : records) {
            if(record.getString("collection").isEmpty() || record.getString("collection") == null) {
                setAutoPublish(record.getString("ds"), record.getBoolean("auto"));
            } else {
                setAutoPublish(record.getString("ds"), record.getString("collection"), record.getBoolean("auto"));
            }
        }
    }

    //TOOD: Get this cron to work on a non mac computer
//    @Scheduled(cron = "*/15 * * * * *")
//    public void autoPublishTimer() {
//        System.out.println("Timer called");
//        publishQueue.forEach(dsc -> tableauTdeManager.createAndPublishTde(dsc.getDs(), dsc.getCollection()));
//        publishQueue.clear();
//        System.out.println("Publish queue cleared");
//    }

    public JSONObject requiresTableauSync() {
        JSONObject jsonObject = new JSONObject();

        JSONArray jsonArray = new JSONArray();
        publishQueue.forEach(dsc -> {
            JSONObject element = new JSONObject();
            element.put("ds", dsc.getDs());
            element.put("c", dsc.getCollection());
            jsonArray.put(element);
        });

        jsonObject.put("changes", jsonArray);

        publishQueue.clear();

        return jsonObject;
    }

    public void setAutoPublishOn(final String datastore) {
        setAutoPublish(datastore, true);
    }

    public void setAutoPublishOff(final String datastore) {
        setAutoPublish(datastore, false);
    }

    public void setAutoPublishOn(final String datastore, final String collection) {
        setAutoPublish(datastore, collection,true);
    }

    public void setAutoPublishOff(final String datastore, final String collection) {
        setAutoPublish(datastore, collection, false);
    }

    /**
     * Returns the auto-publish status of the collection. Status at collection level takes priority over status at
     * datastore level, and status at datastore level takes priority over default status. If no status set at datastore
     * or collection level then default status will be used.
     *
     * @param datastore name of datastore
     * @param collection name of collection
     * @return computed value of auto-publish mode for the specified collection
     */
    public boolean autoPublish(final String datastore, final String collection) {
        boolean autoPublish = defaultValue;

        if(dsMap.containsKey(datastore)) {
            autoPublish = dsMap.get(datastore);
        }

        if(collectionMap.containsKey(datastore) && collectionMap.get(datastore).containsKey(collection)) {
            autoPublish = collectionMap.get(datastore).get(collection);
        }

        return autoPublish;
    }

    public void notifyDataChange(final String datastore, final String collection) {
        if(!FeatureRules.TABLEAU_ENABLED) return;
        if(datastore.equals(BSql.SYSTEM_DB)) {
            return;
        }
        publishQueue.add(new DsAndC(datastore, collection));
    }

    private void setAutoPublish(final String datastore, final Boolean on) {
        dsMap.put(datastore, on);

        if(!collectionMap.containsKey(datastore)) {
            return;
        }

        collectionMap.get(datastore).keySet().forEach(collection -> setAutoPublish(datastore, collection, on));
    }

    private void setAutoPublish(final String datastore, final String collection, final Boolean on) {
        if(!collectionMap.containsKey(datastore)){
            collectionMap.put(datastore, new HashMap<>());
        }

        collectionMap.get(datastore).put(collection, on);
    }
}

class DsAndC {
    private final String ds;
    private final String collection;

    public DsAndC(final String ds, final String collection) {
        this.ds = ds;
        this.collection = collection;
    }

    public String getDs() {
        return ds;
    }

    public String getCollection() {
        return collection;
    }

    @Override
    public boolean equals(Object obj) {
        DsAndC instance = (DsAndC) obj;
        return instance.ds.equals(this.ds) && instance.collection.equals(this.collection);
    }
}
