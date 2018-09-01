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

package com.blobcity.db.operations;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.PathUtil;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.scheduling.annotation.Scheduled;

/**
 *
 * @author sanketsarang
 */
@Component
public class OperationsFileStore {

    /**
     * Map of opid -> MappedByteBuffer containing current value of operations file.
     */
    private final Map<String, JSONObject> jsonMap = new HashMap<>();
    private final Map<String, Path> pathMap = new HashMap<>();
    private final Set<String> modifiedSet = new HashSet<>();

//    @Lock(LockType.WRITE)
    public void load(String appId, String table, String opid) throws OperationException {
        JSONObject jsonObject;
        Path path = FileSystems.getDefault().getPath(PathUtil.operationFile(appId, table, opid));
        String contents;
        try {
            contents = new String(Files.readAllBytes(path));
            jsonObject = new JSONObject(contents);
            jsonMap.put(opid, jsonObject);
            pathMap.put(opid, path);
        } catch (IOException | JSONException ex) {
            LoggerFactory.getLogger(OperationsFileStore.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.OPERATION_FILE_ERROR);
        }
    }

//    @Lock(LockType.WRITE)
    public void unload(final String opid) throws OperationException {
        if (modifiedSet.contains(opid)) {
            dumpToFile(opid);
            modifiedSet.remove(opid);
        }

        jsonMap.remove(opid);
        pathMap.remove(opid);
    }

//    @Lock(LockType.READ)
    public boolean contains(final String opid) {
        return jsonMap.containsKey(opid);
    }

//    @Lock(LockType.WRITE)
    public void update(final String opid, final String contents, final boolean immediate) throws OperationException {
        JSONObject jsonObject;
        if (!jsonMap.containsKey(opid)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_LOADED);
        }
        try {
            jsonObject = new JSONObject(contents);
            jsonMap.put(opid, jsonObject);
            if (immediate) {
                dumpToFile(opid);
            } else {
                modifiedSet.add(opid);
            }
        } catch (JSONException ex) {
            LoggerFactory.getLogger(OperationsFileStore.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INVALID_OPERATION_FORMAT);
        }
    }

//    @Lock(LockType.WRITE)
    public void update(final String opid, final String key, final Object value, final boolean immediate) throws OperationException {
        if (!jsonMap.containsKey(opid)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_LOADED);
        }

        JSONObject jsonObject = jsonMap.get(opid);
        try {
            jsonObject.put(key, value);
            jsonMap.put(opid, jsonObject);
            if (immediate) {
                dumpToFile(opid);
            } else {
                modifiedSet.add(opid);
            }
        } catch (JSONException ex) {
            LoggerFactory.getLogger(OperationsFileStore.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

//    @Lock(LockType.WRITE)
    public void update(final String opid, final JSONObject jsonObject, final boolean immediate) throws OperationException {
        if (!jsonMap.containsKey(opid)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_LOADED);
        }

        jsonMap.put(opid, jsonObject);

        if (immediate) {
            dumpToFile(opid);
        } else {
            modifiedSet.add(opid);
        }
    }

//    @Lock(LockType.READ)
    public String getAsString(String opid) throws OperationException {
        if (!jsonMap.containsKey(opid)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_LOADED);
        }

        return jsonMap.get(opid).toString();
    }

//    @Lock(LockType.READ)
    public JSONObject getAsJson(String opid) throws OperationException {
        if (!jsonMap.containsKey(opid)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_LOADED);
        }

        return jsonMap.get(opid);
    }

    private void dumpToFile(String opid) throws OperationException {
        if (!pathMap.containsKey(opid) || !jsonMap.containsKey(opid)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_LOADED);
        }
        try {
            Files.write(pathMap.get(opid), jsonMap.get(opid).toString().getBytes());
        } catch (IOException ex) {
            LoggerFactory.getLogger(OperationsFileStore.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.OPERATION_FILE_ERROR);
        }
    }

    @Scheduled(fixedRate = 15000)
//    @Lock(LockType.WRITE)
    private void timeout() {
        modifiedSet.stream().forEach((opid) -> {
            try {
                dumpToFile(opid);
            } catch (OperationException ex) {
                LoggerFactory.getLogger(OperationsFileStore.class.getName()).error(null, ex);
            }
        });

        modifiedSet.clear();
    }
}
