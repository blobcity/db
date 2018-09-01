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

package com.blobcity.db.sql.statements;

import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.util.json.JsonMessages;
import com.foundationdb.sql.parser.CreateSchemaNode;
import com.foundationdb.sql.parser.StatementNode;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Good luck figuring out what "Schema" means in our code.
 *
 * @author akshaydewan
 */
@Component
public class CreateSchemaExecutor {

    private static final Integer lock = 0;
    private static final Logger logger = LoggerFactory.getLogger(CreateSchemaExecutor.class);
    private static final List<String> folderPaths = Arrays.asList(
            "code",
            "code/app",
            "code/db",
            "db",
            "del",
            "deploy-db-hot",
            "deployed-versions",
            "deployed-versions/app",
            "deployed-versions/db",
            "logs",
            "logs/compile",
            "uploads"
    );

    private boolean isValidName(final String schemaName) {
        return schemaName.length() <= 128;
    }

    public String execute(StatementNode stmt) throws OperationException {
        logger.trace("CreateSchemaExecutor.execute()");
        CreateSchemaNode createSchemaNode = (CreateSchemaNode) stmt;
        String schemaName = createSchemaNode.getSchemaName();
        if (!isValidName(schemaName)) {
            final String error = "Invalid schema name: " + schemaName;
            logger.debug(error);
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid schema name: " + schemaName);
        }
        //Authorization support to be added
        String folder = BSql.BSQL_BASE_FOLDER;
        folder += "/" + schemaName;
        logger.debug("Creating schema with name: " + schemaName);
        synchronized (lock) {
            File file = new File(folder);
            if (file.exists()) {
                final String error = "The schema name " + schemaName + " already isPresent";
                logger.info(error);
                throw new OperationException(ErrorCode.DATASTORE_ALREADY_EXISTS, error);
            }
            if (!file.mkdir()) {
                final String error = "Could not create root db folder for schema: " + schemaName;
                logger.error(error);
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, error);
            }
            final String dbDirectory = folder + "/";
            for (String key : folderPaths) {
                file = new File(dbDirectory + key);
                if (!file.mkdir()) {
                    final String error = "Could not create folder with key: " + key + " for schema: " + schemaName;
                    logger.error(error);
                    throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, error);
                }
            }
        }
        return JsonMessages.SUCCESS_ACKNOWLEDGEMENT;
    }

}
