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

package com.blobcity.db.master.executors.data;

import com.blobcity.lib.data.Record;
import com.blobcity.lib.query.QueryParams;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author sanketsarang
 */
public class InsertStatusHolder {
    private Integer replicas;
    private final Map<String, List<Record>> recordMap = new ConcurrentHashMap<>();
    private final Map<String, List<Integer>> statusMap = new ConcurrentHashMap<>();
    private final Map<Record, Integer> successMap = new ConcurrentHashMap<>();

//    public InsertStatusHolder(int replicationFactor) {
//        this.replicas = replicationFactor + 1;
//    }

    public void setReplicationFactor(final int replicationFactor) {
        this.replicas = replicationFactor + 1;
    }

    public void addRecords(final String nodeId, final List<Record> recordList) {
        recordMap.put(nodeId, recordList);
    }

    public void addStatus(final String nodeId, final List<Integer> statusList) {
        statusMap.put(nodeId, statusList);
    }

    public boolean allInsertsConsistent() {
        recordMap.forEach((nodeId, records) -> {
            List<Integer> statusList = statusMap.get(nodeId);

            for(int i = 0; i < records.size(); i++) {
                Record record = records.get(i);
                int status = statusList.get(i);

                if(status == 1) {
                    if(!successMap.containsKey(record)) {
                        successMap.put(record, 0);
                    }

                    successMap.put(record, successMap.get(record) + 1);
                }
            }
        });

        return !successMap.values().stream()
                .filter(value -> value != 0 && value != replicas).findFirst()
                .isPresent();
    }

    public JSONObject produceResponsePayload(final List<Record> recordOrderList) {
        final JSONObject payloadJson = new JSONObject();
        final JSONArray statusArray = new JSONArray();
        final JSONArray idArray = new JSONArray();
        int successCount = 0;
        int failedCount = 0;

        for(Record record : recordOrderList) {
            if(successMap.get(record) == replicas) {
                statusArray.put(1);
                idArray.put(record.getId());
                successCount ++;
            } else {
                statusArray.put(0);
                idArray.put("");
                failedCount ++;
            }
        }

        payloadJson.put(QueryParams.STATUS.getParam(), statusArray);
        payloadJson.put(QueryParams.IDS.getParam(), idArray);
        payloadJson.put(QueryParams.INSERTED.getParam(), successCount);
        payloadJson.put(QueryParams.FAILED.getParam(), failedCount);

        return payloadJson;
    }

    public void invalidate() {
        recordMap.clear();
        statusMap.clear();
        successMap.clear();
    }
}
