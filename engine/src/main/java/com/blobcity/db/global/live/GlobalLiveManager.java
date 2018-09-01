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

package com.blobcity.db.global.live;

import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.operations.OperationsManager;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.util.BSqlFileNameFilter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.context.annotation.Lazy;

/**
 * Used to operate on live execution items that need to tracked on a global level and are persisted inside {@link BSql.GLOBAL_LIVE_FOLDER}
 *
 * @author sanketsarang
 */
@Component
public class GlobalLiveManager {

    @Autowired
    @Lazy
    private GlobalLiveStore globalLiveStore;

    public void register(final String id, final JSONObject jsonObject) throws OperationException {
        final String app;
        try {
            app = jsonObject.getString("app");
        } catch (JSONException ex) {
            LoggerFactory.getLogger(GlobalLiveManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
        writeFile(id, jsonObject.toString());
        globalLiveStore.add(app, id);
    }

    public JSONObject get(final String id) throws OperationException {
        final String contents = readFile(id);
        try {
            return new JSONObject(contents);
        } catch (JSONException ex) {
            LoggerFactory.getLogger(GlobalLiveManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    public List<String> getAll() {
        File file = new File(BSql.GLOBAL_LIVE_FOLDER);
        String[] fileNames = file.list(new BSqlFileNameFilter());
        List<String> list = new ArrayList<>();
        list.addAll(Arrays.asList(fileNames));
        return list;
    }

    public void remove(final String id) throws OperationException {
        globalLiveStore.removeOperation(id);
        try {
            Files.deleteIfExists(FileSystems.getDefault().getPath(PathUtil.globalLiveFile(id)));
        } catch (IOException ex) {
            LoggerFactory.getLogger(GlobalLiveManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private void writeFile(final String fileName, final String contents) throws OperationException {
        try {
            Files.write(FileSystems.getDefault().getPath(PathUtil.globalLiveFile(fileName)), contents.getBytes());
        } catch (IOException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error("Could not read global live file: " + fileName, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private String readFile(final String fileName) throws OperationException {
        try {
            return Files.readAllLines(FileSystems.getDefault().getPath(PathUtil.globalLiveFile(fileName)), Charset.forName("UTF-8")).get(0);
        } catch (IOException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error("Could not read global live file: " + fileName, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
}
