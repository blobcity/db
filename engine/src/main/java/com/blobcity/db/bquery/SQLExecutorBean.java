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

package com.blobcity.db.bquery;

import com.blobcity.db.billing.SelectActivityLog;
import com.blobcity.db.security.SecurityManagerBean;
import com.blobcity.db.sql.statements.*;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryData;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryStore;
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.util.JSONOperationException;
import com.blobcity.lib.database.bean.manager.interfaces.engine.SqlExecutor;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * This bean is responsible to execute SQL queries (CRUD related only) inside database
 *
 * @author akshaydewan
 * @author Prikshit Kumar
 * @author sanketsarang
 */
@Component
public class SQLExecutorBean implements SqlExecutor {

    private static final Logger logger = LoggerFactory.getLogger(SQLExecutorBean.class.getName());

    @Autowired @Lazy
    private BSqlCollectionManager collectionManager;
    @Autowired
    private SecurityManagerBean securityManager;
    // Statement Executors---------------------------------------------------------------------------------------------
    @Autowired @Lazy
    private SelectExecutor selectExecutor;
    @Autowired @Lazy
    private UpdateExecutor updateExecutor;
    @Autowired @Lazy
    private DeleteExecutor deleteExecutor;
    @Autowired @Lazy
    private CreateTableExecutor createTableExecutor;
    @Autowired @Lazy
    private AlterTableExecutor alterTableExecutor;
    @Autowired @Lazy
    private DropTableExecutor dropTableExecutor;
    @Autowired @Lazy
    private CreateSchemaExecutor createSchemaExecutor;
    @Autowired
    private QueryStore requestStore;
    @Autowired
    private SelectActivityLog selectActivityLog;

    @Override
    public String runQuery(final String requestId, final String username, final String password, final String datastore, final String sqlString) {
        //if (!licenseBean.isActive()) {
        //  OperationException licenseException = new OperationException(ErrorCode.INVALID_LICENSE);
        //return JSONOperationException.create(licenseException).toString();
        //}

        if(securityManager.verifyCredentials(username, password)) {
            return runQuery(requestId, datastore, sqlString);
        } else {
            return JSONOperationException.create(new OperationException(ErrorCode.USER_CREDENTIALS_INVALID)).toString();
        }
    }

    public String executePrivileged(final String datastore, final String sql) {
        return runQuery("", datastore, sql);
    }

    //This method will be made private and renamed later
    private String runQuery(final String requestId, final String datastore, final String sqlString) {
        if (StringUtils.isBlank(datastore) || StringUtils.isBlank(sqlString)) {
            return JSONOperationException.create(new OperationException(ErrorCode.INVALID_QUERY)).toString();
        }
        StatementNode stmt = null;

        final String tempRequestId = UUID.randomUUID().toString();
        final long startTime = System.currentTimeMillis();
        logger.debug("SQL Query ({}): {}", tempRequestId, sqlString);

        requestStore.register(datastore, tempRequestId, new QueryData(sqlString, startTime));

        try {

            SQLParser parser = new SQLParser();
            try {
                stmt = parser.parseStatement(sqlString);
            } catch (StandardException ex) {
                String msg = "Invalid SQL. ParseStatement failed: " + sqlString + ". " + ex.getMessage();
                logger.info(msg, ex);
                return new JSONObject().put("ack", "0").put("cause", msg).toString();
            }

            try {
                switch (stmt.getNodeType()) {
                    case NodeTypes.CURSOR_NODE:
                        return selectExecutor.execute(datastore, stmt, sqlString);
                    case NodeTypes.UPDATE_NODE:
                        return updateExecutor.execute(datastore, stmt);
                    case NodeTypes.DELETE_NODE:
                        return deleteExecutor.execute(datastore, stmt);
                    case NodeTypes.CREATE_TABLE_NODE:
                        return createTableExecutor.execute(datastore, stmt);
                    case NodeTypes.ALTER_TABLE_NODE:
                        return alterTableExecutor.execute(datastore, stmt);
                    case NodeTypes.DROP_TABLE_NODE:
                        return dropTableExecutor.execute(datastore, stmt);
                    case NodeTypes.CREATE_SCHEMA_NODE:
                        return createSchemaExecutor.execute(stmt);
                    default:
                        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Unsupported statement type: "
                                + stmt.getNodeType()
                                + ". "
                                + sqlString);
                }
            } catch (OperationException e) {
                return JSONOperationException.create(e).toString();
            } catch (IOException ex) {
                logger.error(null ,ex);
            }
        } finally {
            long executionTime = System.currentTimeMillis() - startTime;
            requestStore.unregister(datastore, tempRequestId);

            /* Store select activity for Query Performance Analysis */
            if(stmt != null && stmt.getNodeType() == NodeTypes.CURSOR_NODE && !datastore.equals(".systemdb")) {
                selectActivityLog.registerSelectQuery(datastore, "", sqlString, -1, executionTime);
            }

            logger.debug("SQL Query ({}, {}) Executed in (ms): {} ", requestId, tempRequestId, executionTime);
        }
        return null;
    }

    // Setters for EJBs (used for setting mocks in unit tests ---------------------------------------------------------
    public void setTableManager(BSqlCollectionManager tableManager) {
        this.collectionManager = tableManager;
    }

}
