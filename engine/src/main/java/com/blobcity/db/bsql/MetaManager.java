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

package com.blobcity.db.bsql;

import com.blobcity.db.constants.BSql;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * <p>
 * Stores meta information pertaining to a database within a BlobCity Application. The meta information includes storage and bandwidth consumption</p>
 *
 * <p>
 * The data is stored in the file {appId}/db/meta.bdb in JSON format. The keys of the JSON object are as follows:
 * <ul><li>
 * storage
 * </li><li>
 * bandwidth
 * </li></ul>
 *
 * <p>
 * This storage structure may be extended further to save query, performance, subscription and node specific information when running clustered instances.</p>
 *
 * <p>
 * <b>Note:</b> There is one instance of this file per storage node, hence the total database specific meta information is a collection of meta information from
 * all nodes</p>
 *
 * @author sanketsarang
 */
@Component
public class MetaManager {

    private static final String STORAGE_KEY = "storage";
    private static final String BANDWIDTH_KEY = "bandwidth";
    private static final Logger logger = LoggerFactory.getLogger(MetaManager.class.getName());

    public double getDatabaseSize(String appId) {
        File file = new File(BSql.BSQL_BASE_FOLDER + appId + "/db/meta.bdb");
        JSONObject jsonObject;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            jsonObject = new JSONObject(reader.readLine());
            return jsonObject.getDouble(STORAGE_KEY);
        } catch (IOException | JSONException ex) {
            //do nothing
        }

        return -1.0;
    }

    public double getBandwidthConsumed(String appId) {
        File file = new File(BSql.BSQL_BASE_FOLDER + appId + "/db/meta.bdb");
        JSONObject jsonObject;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            jsonObject = new JSONObject(reader.readLine());
            return jsonObject.getDouble(BANDWIDTH_KEY);
        } catch (IOException | JSONException ex) {
            //do nothing
        }

        return -1.0;
    }

    public double getNumberOfTables(String appId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public double getNumberOfRecords(String appId, String tableName) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void setDatabaseSize(String appId, double size) {
        File file = new File(BSql.BSQL_BASE_FOLDER + appId + "/db/meta.bdb");
        JSONObject jsonObject;

        /* load file data to jsonObject if files is present */
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            jsonObject = new JSONObject(reader.readLine());
        } catch (IOException | JSONException ex) {
            jsonObject = new JSONObject();
        }

        try {
            jsonObject.put(STORAGE_KEY, size);
        } catch (JSONException ex) {
            logger.error(null, ex);
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(jsonObject.toString());
            writer.newLine();
        } catch (IOException ex) {
            logger.error(null, ex);
        }
    }

    public void setBandwidthConsumed(String appId, double bandwidth) {
        File file = new File(BSql.BSQL_BASE_FOLDER + appId + "/db/meta.bdb");
        JSONObject jsonObject;

        /* load file data to jsonObject if files is present */
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            jsonObject = new JSONObject(reader.readLine());
        } catch (IOException | JSONException ex) {
            jsonObject = new JSONObject();
        }

        try {
            jsonObject.put(BANDWIDTH_KEY, bandwidth);
        } catch (JSONException ex) {
            logger.error(null, ex);
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(jsonObject.toString());
            writer.newLine();
        } catch (IOException ex) {
            logger.error(null, ex);
        }
    }
}
