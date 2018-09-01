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
import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Note: This class needs to be evaluated for usefulness. This class probably needs to be deprecated.
 *
 * @author sanketsarang
 */
@Component
@Deprecated
public class OperationLogger {
    private static final Logger logger = LoggerFactory.getLogger(OperationLogger.class);

    private final Map<String, File> fileMap = new HashMap<>();
    private final Map<String, List<String>> delayedMessageMap = new HashMap<>();
    private final Map<String, OperationLogLevel> logLevelMap = new HashMap<>();
    private final Set<String> modifiedSet = new HashSet<>();
    private final Set<String> removeCacheSet = new HashSet<>();

    public void start(String appId, String table, String opid, OperationLogLevel logLevel) throws OperationException {
        logger.trace("OperationLogger.start(ds={0}, collection={1}, opid={2}, logLevel={4}", appId, table, opid, logLevel.getText());

        File file = new File(PathUtil.operationLogFile(appId, table, opid));
        try {
            if (!file.exists() && !file.createNewFile()) {
                throw new OperationException(ErrorCode.OPERATION_LOGGING_ERROR);
            }
        } catch (IOException ex) {
            LoggerFactory.getLogger(OperationLogger.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.OPERATION_LOGGING_ERROR);
        }

        fileMap.put(opid, file);
        delayedMessageMap.put(opid, new ArrayList<>());
        logLevelMap.put(opid, logLevel);
    }

    @Deprecated
    public void delayedLog(String opid, String message) throws OperationException {
        if (!fileMap.containsKey(opid) || !delayedMessageMap.containsKey(opid)) {
            return;
        }

        delayedMessageMap.get(opid).add(message);
    }

    public void log(OperationLogLevel level, String opid, String message) throws OperationException {
        if (!fileMap.containsKey(opid)) {
            return;
        }

        if (level.getLogOrder() < logLevelMap.get(opid).getLogOrder()) {
            return;
        }

        try {
            Files.append(message, fileMap.get(opid), Charset.forName("UTF8"));
        } catch (IOException ex) {
            LoggerFactory.getLogger(OperationLogger.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    public void delayedLog(OperationLogLevel level, String opid, String message) throws OperationException {
        if (!fileMap.containsKey(opid) || !delayedMessageMap.containsKey(opid)) {
            return;
        }

        if (level.getLogOrder() < logLevelMap.get(opid).getLogOrder()) {
            return;
        }

        delayedMessageMap.get(opid).add(message);
        modifiedSet.add(opid);
    }

    public void stop(String opid) throws OperationException {
        removeCacheSet.add(opid);
    }

    @Scheduled(fixedRate = 15000)
    public void executeDelayedLog() {

        modifiedSet.stream().forEach((opid) -> {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileMap.get(opid)))) {
                for (String line : delayedMessageMap.get(opid)) {
                    writer.write(line);
                    writer.newLine();
                }

                delayedMessageMap.get(opid).clear();
            } catch (IOException ex) {
                LoggerFactory.getLogger(OperationLogger.class.getName()).error(null, ex);
            }
        });

        removeCacheSet.stream().map((key) -> {
            fileMap.remove(key);
            return key;
        }).map((key) -> {
            delayedMessageMap.remove(key);
            return key;
        }).forEach((key) -> {
            logLevelMap.remove(key);
        });

        modifiedSet.clear();
        removeCacheSet.clear();
    }
}
