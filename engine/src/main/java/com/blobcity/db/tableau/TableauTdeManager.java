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

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Column;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.sql.util.PathUtil;
//import com.tableausoftware.TableauException;
//import com.tableausoftware.common.Collation;
//import com.tableausoftware.common.Type;
//import com.tableausoftware.extract.*;
//import com.tableausoftware.server.ServerAPI;
//import com.tableausoftware.server.ServerConnection;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * @author sanketsarang
 */
@Component
@EnableAsync
@Deprecated
public class TableauTdeManager {
    private static final Logger logger = LoggerFactory.getLogger(TableauTdeManager.class);
    private static final Semaphore semaphore = new Semaphore(1);

    @Autowired
    private SchemaStore schemaStore;
    @Autowired
    private BSqlDataManager dataManager;
    @Autowired @Lazy
    private TableauPublishStore tableauPublishStore;


    public void createAndPublishTdes(final String datastore) {
        //TODO: Implement this
    }

    @Async
    public void createAndPublishTde(final String datastore, final String collection) {

        //stop-gap solution that simply marks the TDE for publish. External program does the actual publish
        tableauPublishStore.notifyDataChange(datastore, collection);

//        try {
//            semaphore.acquireUninterruptibly();
//
//            try {
//                ExtractAPI.initialize();
//            } catch (TableauException e) {
//                e.printStackTrace();
//                throw new OperationException(ErrorCode.TABLEAU_EXCEPTION);
//            }
//            syncSchema(datastore, collection);
//            populateData(datastore, collection);
//            pushTdeToServer(datastore, collection);
//            deleteTde(datastore, collection);
//
//        } catch(Exception ex) {
//            logger.error("", ex);
//        } finally {
//            semaphore.release();
//        }
    }

    private void syncSchema(final String datastore, final String collection) throws OperationException {
//        Schema collectionSchema = schemaStore.getSchema(datastore, collection);
//        Collection<Column> columns = collectionSchema.getColumnMap().values();
//        String tdeFile = PathUtil.tableauTdeFile(datastore, collection);
//
//        try {
//            Extract extract = new Extract(tdeFile);
//            TableDefinition schema = new TableDefinition();
//            schema.setDefaultCollation( Collation.EN_GB );
//
//            for(Column column : columns) {
//                switch(column.getFieldType().getType()) {
//                    case SMALLINT:
//                    case INT:
//                    case INTEGER:
//                    case BIGINT:
//                    case LONG:
//                        schema.addColumn(column.getName(), Type.INTEGER);
//                        break;
//                    case CHAR:
//                    case CHARACTER:
//                    case CHARACTER_VARYING:
//                    case CHAR_VARYING:
//                    case VARCHAR:
//                    case CHARACTER_LARGE_OBJECT:
//                    case CHAR_LARGE_OBJECT:
//                    case CLOB:
//                    case NATIONAL_CHARACTER:
//                    case NATIONAL_CHAR:
//                    case NCHAR:
//                    case NATIONAL_CHARACTER_VARYING:
//                    case NATIONAL_CHAR_VARYING:
//                    case NCHAR_VARYING:
//                    case NATIONAL_CHARACTER_LARGE_OBJECT:
//                    case NCHAR_LARGE_OBJECT:
//                    case NCLOB:
//                    case STRING:
//                    case BINARY_LARGE_OBJECT:
//                    case BLOB:
//                        schema.addColumn(column.getName(), Type.CHAR_STRING);
//                        break;
//                    case NUMERIC:
//                    case DECIMAL:
//                    case DEC:
//                    case FLOAT:
//                    case REAL:
//                    case DOUBLE_PRECISION:
//                    case DOUBLE:
//                        schema.addColumn(column.getName(), Type.DOUBLE);
//                        break;
//                    case BOOLEAN:
//                        schema.addColumn(column.getName(), Type.BOOLEAN);
//                        break;
//                    case DATE:
//                    case TIME:
//                    case TIMESTAMP:
//                    case INTERVAL:
//                    case REF:
//                    case MULTISET:
//                    case ROW:
//                    case XML:
//                    case ARRAY:
//                    case LIST_INTEGER:
//                    case LIST_FLOAT:
//                    case LIST_LONG:
//                    case LIST_DOUBLE:
//                    case LIST_STRING:
//                    case LIST_CHARACTER:
//
//                        //TODO: Support these types
//                        break;
//                }
//            }
//
//            Table table = extract.addTable( "Extract", schema );
//            if (table == null ) {
//                throw new OperationException(ErrorCode.TABLEAU_EXCEPTION);
//            }
//            extract.close();
//        } catch (TableauException e) {
//            throw new OperationException(ErrorCode.TABLEAU_EXCEPTION);
//        }
    }

    private void populateData(final String datastore, final String collection) throws OperationException {
//        String columnName;
//        String tdeFile = PathUtil.tableauTdeFile(datastore, collection);
//        try {
//            //  Get Schema
//            Extract extract = new Extract(tdeFile);
//            Table table = extract.openTable( "Extract" );
//            TableDefinition tableDef = table.getTableDefinition();
//            final int columnCount = tableDef.getColumnCount();
//
//            List<String> keys = dataManager.selectAllKeys(datastore, collection);
//            for(String key : keys) {
//                JSONObject recordJson = dataManager.select(datastore, collection, key);
//
//                Row row = new Row(tableDef);
//                for(int i=0; i < columnCount; i++) {
//                    columnName = tableDef.getColumnName(i);
//                    switch(tableDef.getColumnType(i)) {
//                        case INTEGER:
//                            row.setLongInteger(i, recordJson.getLong(columnName));
//                            break;
//                        case DOUBLE:
//                            row.setDouble(i, recordJson.getDouble(columnName));
//                            break;
//                        case BOOLEAN:
//                            row.setBoolean(i, recordJson.getBoolean(columnName));
//                        case CHAR_STRING:
//                            row.setCharString(i, recordJson.getString(columnName));
//                            break;
//                        case UNICODE_STRING:
//                            row.setString(i, recordJson.getString(columnName));
//                            break;
//                        case DATE:
//                        case DATETIME:
//                        case DURATION:
//                        case SPATIAL:
//
//                            //TODO: Not yet supported
//                            break;
//                    }
//                }
//
//                table.insert(row);
//            }
//            extract.close();
//        } catch ( TableauException e ) {
//            throw new OperationException(ErrorCode.TABLEAU_EXCEPTION);
//        }
    }

    private void pushTdeToServer(final String datastore, final String collection) throws OperationException {

        System.out.println("TODO: Implement the publish phase");

//        try {
//
//            ServerAPI.initialize();
//
//            // Create the server connection object
//            ServerConnection serverConnection = new ServerConnection();
//
//            // Connect to the server
//            serverConnection.connect("https://visual.blobcity.com", "apiuser", "Dcs-H5H-mkz-LmY",  datastore);
//
//            // Publish order-java.tde to the server under the default project with name Order-java
//            serverConnection.publishExtract(PathUtil.tableauTdeFile(datastore, collection), "Default", collection, true);
//
//            // Disconnect from the server
//            serverConnection.disconnect();
//
//            // Destroy the server connection object
//            serverConnection.close();
//        }
//        catch ( TableauException e ) {
//            e.printStackTrace();
//            throw new OperationException(ErrorCode.TABLEAU_EXCEPTION);
//        }
    }

    private void deleteTde(final String datastore, final String collection) throws OperationException {
        final String tdeFile = PathUtil.tableauTdeFile(datastore, collection);
        try {
            Files.deleteIfExists(FileSystems.getDefault().getPath(tdeFile));
        } catch (IOException e) {
            throw new OperationException(ErrorCode.TABLEAU_EXCEPTION);
        }
    }
}
