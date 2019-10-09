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

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.BSqlDatastoreManager;
import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.cli.statements.DDLStatement;
import com.blobcity.db.cluster.nodes.NodeManager;
import com.blobcity.db.code.CodeExecutor;
import com.blobcity.db.code.CodeLoader;
import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.export.ExportType;
import com.blobcity.db.ftp.FtpServiceManager;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import com.blobcity.db.features.FeatureRules;
import com.blobcity.db.mapreduce.MapReduceExecutor;
import com.blobcity.db.mapreduce.MapReduceJobManager;
import com.blobcity.db.mapreduce.MapReduceOutputImporter;
import com.blobcity.db.memory.old.MemoryTableStore;
import com.blobcity.db.olap.DataCubeManager;
import com.blobcity.db.operations.OperationLogLevel;
import com.blobcity.db.operations.OperationTypes;
import com.blobcity.db.operations.OperationsManager;
import com.blobcity.db.requests.RequestHandlingBean;
import com.blobcity.db.schema.*;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.security.SecurityManagerBean;
import com.blobcity.db.security.exceptions.BadPasswordException;
import com.blobcity.db.security.exceptions.BadUsernameException;
import com.blobcity.db.security.exceptions.InvalidCredentialsException;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.storage.BSqlMemoryManagerOld;
import com.blobcity.db.security.UserGroup;
import com.blobcity.db.security.UserGroupManager;
import com.blobcity.db.tableau.TableauCommands;
import com.blobcity.db.tableau.TableauPublishManager;
import com.blobcity.db.tableau.TableauTdeManager;
import com.blobcity.db.watchservice.WatchServiceImportType;
import com.blobcity.db.watchservice.WatchServiceManager;
import com.blobcity.lib.database.bean.manager.interfaces.engine.ConsoleExecutor;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryData;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryStore;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collection;
import java.util.Iterator;

import com.blobcity.lib.query.CollectionStorageType;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import com.blobcity.lib.query.RecordType;
import org.apache.log4j.LogManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Processes commands fired from the CLI for operating on the database. Supported command formats can be found at
 * {@link http://docs.blobcity.com/display/DB/CLI+Commands}
 *
 * @author sanketsarang
 * @author Prikshit Kumar
 */
@Component
//@EnableAsync
public class ConsoleExecutorBean implements ConsoleExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleExecutorBean.class);

    @Autowired @Lazy
    private CodeLoader codeLoader;
    @Autowired @Lazy
    private CodeExecutor codeExecutor;
    @Autowired @Lazy
    private BSqlCollectionManager collectionManager;
    @Autowired @Lazy
    private ConfigBean configBean;
    @Autowired @Lazy
    private DataCubeManager dataCubeManager;
    @Autowired @Lazy
    private BSqlDatastoreManager datastoreManager;
    @Autowired @Lazy
    private BSqlDataManager dataManager;
    @Autowired @Lazy
    private DDLStatement ddlStatement;
    @Autowired @Lazy
    private UserGroupManager groupManager;
    @Autowired @Lazy
    private BSqlIndexManager indexManager;
    @Autowired @Lazy
    private BSqlMemoryManagerOld memoryManager;
    @Autowired @Lazy
    private MapReduceExecutor mapReduceExecutor;
    @Autowired @Lazy
    private MapReduceJobManager mapReduceJobManager;
    @Autowired @Lazy
    private MapReduceOutputImporter mapReduceOutputImporter;
    @Autowired @Lazy
    private NodeManager nodeManager;
    @Autowired @Lazy
    private OperationsManager operationsManager;
    @Autowired @Lazy
    private QueryStore queryRequestStore;
    @Autowired @Lazy
    private SQLExecutorBean sqlExecutorBean;
    @Autowired @Lazy
    private SchemaStore schemaStore;
    @Autowired @Lazy
    private SecurityManagerBean securityManager;
    @Autowired @Lazy
    private WatchServiceManager watchServiceManager;
    @Autowired @Lazy
    private RequestHandlingBean requestHandlingBean;
    @Autowired @Lazy
    private FtpServiceManager ftpServiceManager;
    @Autowired @Lazy
    private TableauPublishManager tableauPublishManager;
    @Autowired @Lazy
    private TableauTdeManager tableauTdeManager;

    @Override
    public String insertData(final String user, final String ds, final String collection, final String dataType, final String data) {
        String queryId = UUID.randomUUID().toString();
        logger.debug("Console Insert (" + queryId + ") " + dataType + " into " + ds + "." + collection + ": " + data);

        final List<Object> dataArray = new ArrayList<>();
        dataArray.add(data);
        RecordType recordType = RecordType.fromTypeCode(dataType);
        Query query = new Query().insertQueryUninferred(ds, collection, dataArray, recordType);

        //TODO: Interpreters and interceptors should be added here within the query

        Query responseQuery = requestHandlingBean.newRequest(query);
        if(responseQuery.isAckSuccess()) {
            return "Inserted";
        } else {
            return "Failed insert";
        }
    }

    @Override
    public String runCommand(final String user, String query) {
        if (query == null || query.isEmpty()) {
            return "";
        }

        query = query.trim();
        String queryId = UUID.randomUUID().toString();

        /* remove extra spaces or null elements from the query */
        List<String> els = new ArrayList<>(Arrays.asList(query.split(" ")));
        els.removeAll(Collections.singleton(null));
        els.removeAll(Collections.singleton(""));
        String[] elements = (String[]) els.toArray(new String[els.size()]);

        logger.debug("Console Query (" + queryId + "): " + query);

        /* Check if the user is not root and is attempt to execute a root user only operation */
        if(!FeatureRules.BYPASS_ROOT_ONLY && !user.equals("root")){
            switch(elements[0]) {
                case "list-ds":
                case "add-node":
                case "remove-node":
                case "connect-node":
                case "node-id":
                case "apply-license":
                case "cluster-status":
                case "set-replication":
                case "set-geo-replication":
                    return "Operation restricted";
            }
        }

        String response;
        try {
            switch (elements[0].toLowerCase()) {
            /* node-related commands */
                case "add-node":
                    response = addNode(elements);
                    break;
                case "remove-node":
                    response = removeNode(elements);
                    break;
                case "connect-node":
                    response = connectNode(elements);
                    break;
                case "node-id":
                    response = nodeId(elements);
                    break;
                case "cluster-status":
                    response = clusterStatus();
                    break;
                case "shutdown":
                    response = shutdown();
                    break;

            /* indexing related commands */
                case "create-index":
                    response = createIndex(elements);
                    break;
                case "drop-index":
                    response = dropIndex(elements);
                    break;

            /* dsSet related commands */
                case "create-db":
                case "create-ds":
                case "create-dsSet":
                    response = createDb(elements);
                    break;
                case "truncate-db":
                case "truncate-ds":
                case "truncate-dsSet":
                    response = truncateDb(elements);
                    break;
                case "drop-db":
                case "drop-ds":
                case "drop-dsSet":
                    response = dropDb(elements);
                    break;
                case "list-db":
                case "list-ds":
                case "list-dsSet":
                    response = listDS(elements);
                    break;

            /* collection related commands */
                case "create-table":
                case "create-collection":
                    response = createTable(elements);
                    break;
                case "truncate-table":
                case "truncate-collection":
                    response = truncateTable(elements);
                    break;
                case "drop-table":
                case "drop-collection":
                    response = dropTable(elements);
                    break;
                case "populate-table":
                case "populate-collection":
                    response = populateTable(elements);
                    break;
                case "repopulate-table":
                case "repopulate-collection":
                    response = populateTable(elements);
                    break;
                case "dump-table":
                case "dump-collection":
                    response = dumpTable(elements);
                    break;
                case "view-table":
                case "view-collection":
                    response = viewTable(elements);
                    break;
                case "list-tables":
                case "list-collections":
                    response = listTables(elements);
                    break;
                case "pop-table":
                case "pop-collection":
                    response = popTable(elements);
                    break;
                case "clear-table":
                case "clear-collection":
                    response = clearTable(elements);
                    break;
                case "set-auto-define":
                    response = setAutoDefine(elements);
                    break;
                case "set-replication":
                    response = setReplication(elements);
                    break;
                case "set-geo-replication":
                    response = setGeoReplication(elements);
                    break;

            /* column related commands */
                case "add-column":
                    response = addColumn(elements);
                    break;
                case "drop-column":
                    response = dropColumn(elements);
                    break;
                case "rename-column":
                    response = renameColumn(elements);
                    break;
                case "set-column-type":
                    response = setColumnType(elements);
                    break;

            /* user related commands */
                case "create-user":
                    response = createUser(elements);
                    break;
                case "set-user-password":
                    response = setUserPassword(elements);
                    break;
                case "drop-user":
                    response = dropUser(elements);
                    break;
                case "create-master-key":
                    response = createMasterKey();
                    break;
                case "create-ds-key":
                    response = createDsKey(elements);
                    break;
                case "list-api-keys":
                    response = listApiKeys();
                    break;
                case "list-ds-api-keys":
                    response = listDsApiKeys(elements);
                    break;
                case "drop-api-key":
                    response = dropApiKey(elements);
                    break;

            /* user group related commands */
                case "create-group":
                    response = createGroup(elements);
                    break;
                case "delete-group":
                    response = deleteGroup(elements);
                    break;
                case "view-group":
                    response = viewGroup(elements);
                    break;
                case "rename-group":
                    response = renameGroup(elements);
                    break;
                case "add-user":
                    response = addUserToGroup(elements);
                    break;
                case "remove-user":
                    response = removeUserFromGroup(elements);
                    break;

            /* user-code related commands */
                case "load-code":
                    response = loadCode(elements);
                    break;
                case "remove-code":
                    response = removeCode(elements);
                    break;
                case "delete-code":
                    response = deleteCode(elements);
                    break;
                case "list-code":
                    response = listCode(elements);
                    break;

            /* trigger related commands */
                case "activate-trigger":
                    response = activateTrigger(elements);
                    break;
                case "deactivate-trigger":
                    response = deactivateTrigger(elements);
                    break;
                case "list-triggers":
                    response = listTriggers(elements);
                    break;

            /* map reduce related commands */
                case "mr-start":
                    response = mrStartJob(elements);
                    break;
                case "mr-status":
                    response = mrJobStatus(elements);
                    break;
                case "mr-info":
                    response = mrJobInfo(elements);
                    break;
                case "mr-history":
                    response = mrJobHistory(elements);
                    break;
                case "mr-cancel":
                    response = mrCancelJob(elements);
                    break;
                case "mr-progress":
                    response = mrJobProgress(elements);
                    break;
                case "mr-import":
                    response = mrImportOutput(elements);
                    break;

            /* misc commands */
                case "sql":
                    /* Query is of the format "sql dbName:SELECT * FROM ....." */
                    String dbAndFilename = query.substring(4, query.indexOf(":", 4));
                    String sqlDbName;
                    String outputFilename;
                    if(dbAndFilename.contains(" ")) {
                        String []parts = dbAndFilename.split(" ");
                        sqlDbName = parts[0];
                        outputFilename = parts[1];
                    } else {
                        sqlDbName = dbAndFilename;
                        outputFilename = null;
                    }
                    response = executeSql(sqlDbName, query.substring(query.indexOf(":", 4) + 1), outputFilename);
                    break;
                case "copy-schema":
                    response = copySchema(elements);
                    break;
                case "query-stats":
                    response = queryStats(elements);
                    break;
                case "gc":
                    response = gc();
                    break;
                case "export-data":
                    response = exportData(elements);
                    break;
                case "import-csv":
                    response = importCSV(elements);
                    break;
                case "import-excel":
                    response = importExcel(elements);
                    break;
                case "insert-custom":
                    response = insertCustom(elements);
                    break;
                case "set-log-level":
                    response = setLogLevel(elements);
                    break;
                case "row-count":
                    response = rowCount(elements);
                    break;

                case "set-interpreters":
                    response = setInterpreters(elements);
                    break;

            /* debug commands */
                case "get-data":
                    response = getData(elements);
                    break;
                case "get-key":
                    response = getKeys(elements);
                    break;
                case "col-mapping":
                    response = getColumnMapping(elements);

            /* Folder and file watch services */
                case "start-watch":
                    response = startWatch(elements);
                    break;
                case "stop-watch":
                    response = stopWatch(elements);
                    break;
                case "list-watch":
                    response = listWatch(elements);
                    break;

                /* FTP services */
                case "start-ds-ftp":
                    response = startDsFtp(elements);
                    break;

                case "stop-ds-ftp":
                    response = stopDsFtp(elements);
                    break;

            /* Tableau */
                case "tableau":
                    response = tableau(elements);
                    break;

            /* Other commands */
                case "help":
                    response = "Visit http://docs.blobcity.com/telnet-cli-interface.html for more details.";
                    break;

                default:
                    response = "Unrecognized command: " + elements[0];
                    break;
            }
        } catch (OperationException ex) {
            response = "[" + ex.getErrorCode() + "] " + ((ex.getMessage() == null || ex.getMessage().isEmpty()) ? ex.getErrorCode().getErrorMessage() : ex.getMessage());
        }
        logger.debug("Console Response (" + queryId + "): " + response);
        return response;
    }

    @Deprecated
    private void verifyDCInfo(final String datastore, final String... collection) throws OperationException {
        if (!datastoreManager.exists(datastore))
            throw new OperationException(ErrorCode.DATASTORE_INVALID);

        if (collection != null) {
            if (!collectionManager.exists(datastore, collection[0]))
                throw new OperationException(ErrorCode.COLLECTION_INVALID);
        }
    }

    private String shutdown() {
        Runtime.getRuntime().halt(1);
        return "Shutdown";
    }

    private String activateTrigger(String[] elements) throws OperationException {
        if (elements.length < 3 || elements.length > 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Activate-triggers command takes exactly two parameters");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }
        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(database, table);
        codeLoader.activateTrigger(database, table, elements[2]);
        if ("*".equals(elements[2])) {
            return "All triggers have been successfully activated";
        }
        return "Trigger " + elements[2] + " successfully activated";
    }

    private String addColumn(String[] elements) throws OperationException {
        // structure: add-column dbname.tablename colname type indexing auto-define
        if (elements.length != 6) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Incorrect Number of parameters provided. Add-Column takes exactly five paarameters");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(database, table);
        final String columnName = elements[2];
        final String typeString = elements[3];
        final IndexTypes indexType = IndexTypes.fromString(elements[4]);
        final AutoDefineTypes autoDefineType = AutoDefineTypes.fromString(elements[5]);
        final FieldType fieldType = FieldTypeFactory.fromString(typeString);
        collectionManager.addColumn(database, table, columnName, fieldType, autoDefineType, indexType);

        return "Column successfully added";
    }

    private String addNode(String[] elements) throws OperationException {
        final String nodeId;
        final String ipAddress;

        switch (elements.length) {
            /* add-node <node-id> */
            case 2:
                nodeId = elements[1];
                nodeManager.addNode(nodeId);
                return "Node " + nodeId + " successfully added";
            /* add-node <node-id> <ip-address>*/
            case 3:
                nodeId = elements[1];
                ipAddress = elements[2];
                nodeManager.addNode(nodeId, ipAddress);
                return "Node " + nodeId + "," + ipAddress + " successfully added";
            default:
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Format for add-node command is> add-node <node-id> <ip-address>");
        }
    }

    private String clusterStatus() {
        String status = "node-id | name | IP | status\n";
        status += UUID.randomUUID().toString() + " | node 1 | localhost1 | up\n";
        status += UUID.randomUUID().toString() + " | node 2 | localhost2 | down\n";
        return status;
    }

    private String connectNode(String[] elements) throws OperationException {
        if (elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Format for connect-node command is> connect-node <node-id> <ip-address>");
        }
        return "Node successfully connected";
    }

    private String copySchema(String[] elements) throws OperationException {
        final String sourceDb;
        final String sourceTable;
        final String destinationDb;
        final String destinationTable;

        if (!elements[1].contains(".")) {
            sourceDb = elements[1].substring(0, elements[1].indexOf("."));
            sourceTable = elements[1].substring(elements[1].indexOf(".") + 1, elements[1].length());
        } else {
            sourceDb = elements[1];
            sourceTable = null;
        }

        if (!elements[2].contains(".")) {
            destinationDb = elements[2].substring(0, elements[2].indexOf("."));
            destinationTable = elements[2].substring(elements[2].indexOf(".") + 1, elements[2].length());
        } else {
            destinationDb = elements[2];
            destinationTable = null;
        }

        if ((sourceTable == null && destinationTable != null)
                || sourceTable != null && destinationTable == null) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "source and destination table must either both be specified or not-specified");
        }

        //TODO: Implement schema copy procedure
        return "Query not supported";
    }

    private String createDb(String[] elements) throws OperationException {
        /* Ensure database parameter is present */
        if (elements.length < 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing dsSet parameter for create-ds command");
        }

        /* Ensure no extra parameters are present */
        if (elements.length > 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "create-ds command takes only a single parameter containing the dsSet name");
        }

        final String database = elements[1];

        /* Ensure database name specified is a valid database name */
        Pattern pattern = Pattern.compile("[0-9a-zA-Z$_$-]+");
        Matcher matcher = pattern.matcher(database);
        if (!matcher.matches()) {
            throw new OperationException(ErrorCode.INVALID_DATASTORE_NAME, database + " is not a valid dsSet name");
        }

        Query query = new Query().createDs(database);
        Query responseQuery = requestHandlingBean.newRequest(query);

        if(responseQuery.isAckSuccess()) {
            return "Datastore successfully created";
        } else {
            return "Failed to create datastore. " + ErrorCode.fromString(responseQuery.getErrorCode()).getErrorMessage();
        }
    }

    private String createIndex(String[] elements) throws OperationException {
        if (elements.length != 4) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid no of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        verifyDCInfo(database, table);

        final String columnName = elements[2];
        final String indexTypeString = elements[3];
        IndexTypes indexType = IndexTypes.fromString(indexTypeString);

        indexManager.index(database, table, columnName, indexType, OperationLogLevel.ERROR);
        return "Added index for " + columnName + " in " + database + "." + table;
    }

    private String createTable(String[] elements) throws OperationException {

        if (elements.length < 1 || elements.length > 4) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid no of arguments provided");
        }

        /* Get database and table name */
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Datastore and collection should be specified in format: datastoreName.collectionName");
        }

        final String ds = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String collection = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());


        /* Get storage type specification */
        CollectionStorageType storageType;
        if(elements.length >= 3) {
            storageType = CollectionStorageType.fromTypeCode(elements[2]);

            if(storageType == null) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Parameter 2 should be storage type with value on off [on-disk,in-memory,in-memory-nd]");
            }
        }else {
            storageType = CollectionStorageType.ON_DISK;
        }

        /* Get replication specifications */
        ReplicationType replicationType = ReplicationType.DISTRIBUTED;
        int replicationFactor = 0;

        if(elements.length >= 4) {
            replicationType = ReplicationType.fromString(elements[3]);

            if(replicationType == null) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Parameter 3 should be replication type with value on off [distributed,mirrored]");
            }
        }

        if(replicationType == ReplicationType.DISTRIBUTED) {
            if(elements.length >= 5) {
                replicationFactor = Integer.valueOf(elements[4]);
            }
        }

        Query query = new Query().createCollection(ds, collection, storageType);
        query.put(QueryParams.REPLICATION_TYPE, replicationType.getType());
        query.put(QueryParams.REPLICATION_FACTOR, replicationFactor);
        Query responseQuery = requestHandlingBean.newRequest(query);

        if(responseQuery.isAckSuccess()) {
            return "Collection successfully created";
        } else {
            return "Failed to create collection. " + ErrorCode.fromString(responseQuery.getErrorCode()).getErrorMessage();
        }
    }

    private String createUser(String[] elements) throws OperationException {
        if (elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        final String username = elements[1];
        final String password = elements[2];

        try {
            securityManager.addUser(username, password);
            return "User successfully added";
        } catch (BadUsernameException ex) {
            // Logger.getLogger(ConsoleExecutorBean.class.getName()).log(Level.SEVERE, null, ex);
            return "User could not be added due to a problem with the username";
        } catch (BadPasswordException ex) {
            // Logger.getLogger(ConsoleExecutorBean.class.getName()).log(Level.SEVERE, null, ex);
            return "User could not be added due to a problem with the password";
        }
    }

    private String setUserPassword(String []elements) throws OperationException {
        if(elements.length != 4) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }

        String user = elements[1];
        String oldPassword = elements[2];
        String newPassword = elements[3];

        try {
            securityManager.changePassword(user, oldPassword, newPassword);
            return "User password updated";
        } catch (InvalidCredentialsException e) {
            return "Invalid old password";
        } catch (BadPasswordException e) {
            return "Password could not be changed due to problem with new password";
        }
    }

    private String deactivateTrigger(String[] elements) throws OperationException {
        if (elements.length < 3 || elements.length > 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "DeActivate-trigger command takes exactly two parameters.");

        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }
        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1));//start searching dot from index 1 to handle case of .systemdb as datastore name
         final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(database, table);
        codeLoader.deActivateTrigger(database, table, elements[2]);
        if ("*".equals(elements[2])) {
            return "All triggers have been succesfully deactivated";
        }
        return "Trigger " + elements[2] + " successfully deactivated";
    }

    private String deleteCode(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "load-code takes only one parameter - name of class file to be loaded in database");
        }
        final String datastore = elements[1];
        verifyDCInfo(datastore);
        codeLoader.deleteAllClasses(datastore);
        return "All classes have been removed from database as well as from filesystem";
    }

    private String dumpTable(String[] elements) throws OperationException {
        String contents = "";
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        if (!MemoryTableStore.exists(databaseAndTable)) {
            return "The specified table does not exist. This operation is only supported for in memory tables";
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(database, table);

        Collection<Object> allRecords = memoryManager.selectAll(database, table);
        contents = allRecords.parallelStream().map((record) -> record.toString()).reduce(contents, String::concat);
        return contents;
    }

    private String dropColumn(String[] elements) throws OperationException {
        if (elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid no of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(database, table);
        final String columnName = elements[2];
        collectionManager.dropColumn(database, table, columnName);

        return "Column successfully dropped";
    }

    private String dropDb(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid no of arguments provided");
        }
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Insufficient number of parameters provided Drop-db takes exactly one argument (dbname)");
        }
        String datastore = elements[1];

        Query query = new Query().dropDs(datastore);
        Query responseQuery = requestHandlingBean.newRequest(query);

        if(responseQuery.isAckSuccess()) {
            return "Datastore dropped successfully";
        } else {
            return "Failed to drop datastore. " + ErrorCode.fromString(responseQuery.getErrorCode()).getErrorMessage();
        }
    }

    private String dropIndex(String[] elements) throws OperationException {
        if (elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid no of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(database, table);
        final String columnName = elements[2];

        indexManager.dropIndex(database, table, columnName);
        return "Removed index for " + columnName + " in " + database + "." + table;
    }

    private String dropTable(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid no of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Datastore and collection should be specified in format: datastoreName.collectionName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        verifyDCInfo(database, table);

        collectionManager.dropTable(database, table);
        return "Collection successfully dropped";
    }

    private String dropUser(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid no of arguments provided");
        }
        final String username = elements[1];

        try {
            securityManager.deleteUser(username);
            return "User successfully dropped";
        } catch (BadUsernameException ex) {
            // Logger.getLogger(ConsoleExecutorBean.class.getName()).log(Level.SEVERE, null, ex);
            return "No user found with the given username";
        }
    }

    private String executeSql(String dbName, String sql, String file) throws OperationException {
        if(file == null || file.isEmpty()) {
            return sqlExecutorBean.executePrivileged(dbName, sql);
        } else {
            final String resultString = sqlExecutorBean.executePrivileged(dbName, sql);

            try(BufferedWriter writer = new BufferedWriter(new FileWriter(new File(PathUtil.datastoreFtpFolder(dbName) + file)))){
                writer.write(resultString);
                return "Query result successfully written to file " + PathUtil.datastoreFtpFolder(dbName) + file;
            } catch (IOException ex){
                return resultString;
            }
        }
    }

    private String exportData(String[] elements) throws OperationException {
        final String databaseAndTable = elements[1];
        final String type = elements[2];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        final String exportFile = elements[3];
        JSONObject jsonObject = new JSONObject();

        if (ExportType.fromString(type) == null) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid 'type' parameter in payload. Value: " + type + " does not match a valid export format");
        }

        /* Make JSON request format consistent with operation file format */
        try {
            jsonObject.put("file", exportFile);
            jsonObject.put("type", OperationTypes.EXPORT.getTypeCode());
            jsonObject.put("export-type", type);
            jsonObject.put("records", 0);
            jsonObject.put("time-started", -1);
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        final String opid = operationsManager.registerOperation(database, table, OperationTypes.EXPORT, jsonObject);

        return "Export started with operation id " + opid + ". Exported file will be named " + exportFile;
    }

    private String gc() {
        System.gc();
        return "Garbage collection requested";
    }

    private String listTables(String[] elements) throws OperationException {
        if (elements.length == 1) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Datastore name missing");
        }
        final String datastore = elements[1];

        Query query = new Query().listCollectionsQuery();
        query.put(QueryParams.DATASTORE, datastore);
        Query responseQuery = requestHandlingBean.newRequest(query);

        if(!responseQuery.isAckSuccess()) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "failed to execute operation");
        }

        return Arrays.toString(((Collection)responseQuery.getPayload()).toArray());

    }

    private String listTriggers(String[] elements) throws OperationException {
        if (elements.length < 2 || elements.length > 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "List-triggers takes one parameter only.");
        }
        String databaseAndTable = elements[1];

        if (databaseAndTable.contains(".")) {
            final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
            final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
            verifyDCInfo(database, table);
            return codeLoader.listTriggers(database, table);
        } else {
            verifyDCInfo(databaseAndTable);
            return codeLoader.listTriggers(databaseAndTable);
        }
    }

    private String loadCode(String[] elements) throws OperationException {
        if (elements.length < 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "load-code requires atleast 2 parameters");
        }
        String datastore = elements[1];
        String jarFilePath = elements[2];

//        verifyDCInfo(datastore);
//        codeLoader.loadAllClasses(datastore);

        codeLoader.loadJar(datastore, jarFilePath);
        return "All classes successfully loaded into the database";
    }

    private String listCode(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "list-codes takes only one parameter - dsSet");
        }
        String datastore = elements[1];
        verifyDCInfo(datastore);
        return codeLoader.listAllCode(datastore);
    }

    private String listDS(String[] elements) throws OperationException {
        if(!FeatureRules.ALLOW_LIST_DS) {
            throw new OperationException(ErrorCode.FEATURE_RESTRICTED, "you do not have necessary permissions to run list-ds");
        }

        if (elements.length != 1) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "list-ds doesn't take any arguments");
        }

        Query query = new Query().listDsQuery();
        Query responseQuery = requestHandlingBean.newRequest(query);

        if(!responseQuery.isAckSuccess()) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "failed to execute operation");
        }

        return Arrays.toString(((Collection)responseQuery.getPayload()).toArray());
    }

    private String nodeId(String[] elements) throws OperationException {
        if (elements.length == 1) {
//                return com.blobcity.license.License.getNodeId();
                return "default"; //temp code until removal of licensing module
        } else if (elements.length == 2) {
            final String ip = elements[1];
            return "Getting node-id by IP is not yet supported";
        } else {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Format for node-id command is> node-id <ip-address>");
        }
    }

    private String queryStats(String[] elements) throws OperationException {
        if (elements.length == 1) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database name missing");
        }
        final String database = elements[1];

        final long currentTime = System.currentTimeMillis();
        JSONObject responseJson = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        responseJson.put("db", database);

        Map<String, QueryData> map = queryRequestStore.getAppQueries(database);
        if (map == null) {
            return "No queries currently executing for database " + database;
        }
        map.forEach((key, value) -> {
            JSONObject itemJson = new JSONObject();
            itemJson.put("q", value.getQuery());
            itemJson.put("tm", currentTime - value.getStartTime());
            jsonArray.put(itemJson);
        });

        responseJson.put("current-time", System.currentTimeMillis());
        responseJson.put("p", jsonArray);
        return responseJson.toString();
    }

    private String removeCode(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "load-code takes only one parameter - name of class file to be loaded in database");
        }
        String datastore = elements[1];
        verifyDCInfo(datastore);
        codeLoader.removeAllClasses(elements[1]);
        return "All classes have been removed from database successfully";
    }

    private String removeNode(String[] elements) throws OperationException {
        final String nodeId;
        final boolean gracefully;
        switch (elements.length) {
            /* remove-node <node-id> */
            case 2:
                nodeId = elements[1];
                gracefully = true;
                nodeManager.removeNode(nodeId, gracefully);
                return "Node " + nodeId + " successfully removed";
            /* remove-node <node-id> <gracefully/immediate> */
            case 3:
                nodeId = elements[2];
                gracefully = Boolean.valueOf(elements[2]);
                nodeManager.removeNode(nodeId, gracefully);
                return "Node " + nodeId + " successfully removed";
            default:
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Format for remove-node command is> remove-node <node-id> <gracefully/immediate>");
        }
    }

    /**
     * synatx: rename-column db.table OldName NewName
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String renameColumn(String[] elements) throws OperationException {
        if (elements.length != 4) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }
        final String datastoreandCollection = elements[1];

        if (!datastoreandCollection.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }
        final String datastore = datastoreandCollection.substring(0, datastoreandCollection.indexOf("."));
        final String collection = datastoreandCollection.substring(datastoreandCollection.indexOf(".") + 1, datastoreandCollection.length());
        verifyDCInfo(datastore, collection);
        collectionManager.renameColumn(datastore, collection, elements[2], elements[3]);
        return datastoreandCollection + "." + elements[2] + " has been successfully renamed to " + datastoreandCollection + "." + elements[3];
    }

    private String truncateDb(String[] elements) throws OperationException {

        //TODO: implement this

        return "Operation not yet supported in version 1.4";

//        if (elements.length == 1) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database name missing");
//        }
//        final String datastore = elements[1];
//        verifyDCInfo(datastore);
//        datastoreManager.truncateDs(datastore);
//        return "Database " + datastore + " successfully truncated";
    }

    private String truncateTable(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Not sufficient Arguments provided");
        }

        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String datastore = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String collection = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(datastore, collection);

        collectionManager.truncateTable(datastore, collection);
        return "Successfully truncated " + datastore + "." + collection;
    }

    private String viewTable(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String datastore = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String collection = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(datastore, collection);
        Schema schema = schemaStore.getSchema(datastore, collection);
        // return table data in readable format
        StringBuilder collectionInfo = new StringBuilder("\n");
        collectionInfo.append("replication-type:\t").append(schema.getReplicationType().toString()).append("\n")
                .append("replication-factor:\t").append(schema.getReplicationFactor()).append("\n")
                .append("table-type:\t\t").append(schema.getTableType().toString()).append("\n")
                .append("flexible-schema:\t").append(schema.isFlexibleSchema()).append("\n")
                .append("colummns:\n")
                .append("\tAuto-define\tIndex\t\tType\t\tName\n");
        for (Column tmp : schema.getColumnMap().values()) {
            collectionInfo.append("\t")
                    .append(tmp.getAutoDefineType().getText()).append("\t\t")
                    .append(tmp.getIndexType().getText()).append("\t\t")
                    .append(tmp.getFieldType().getType().toString()).append("\t\t")
                    .append(tmp.getName()).append("\t");
            if (tmp.getName().equals(schema.getPrimary())) collectionInfo.append("\t\tPRIMARY");
            collectionInfo.append("\n");
        }
        return collectionInfo.toString();
    }

    /* CLI commands related to Map reduce functionality */

    /**
     * Cancel a map-reduce job
     * [1]: job id
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String mrCancelJob(String[] elements) throws OperationException {
        return "Operation depricated";

//        if (elements.length != 2) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
//        }
//        String jobId = elements[1];
//        mapReduceExecutor.cancelJob(elements[1]);
//        return "Map Reduce Job " + jobId + " has been successfully termninated";
    }

    /**
     * import the output of a map reduce job
     * [1]: job id
     * [2]: database where data is to be imported
     * [3]: table in which data is to be imported
     * [4]: external file where mapping of columns in output file is present  (optional)
     * <p> if not specified, it will assume that the first line in the first output file contains the
     * mapping of columns</p>
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String mrImportOutput(String[] elements) throws OperationException {
        if (elements.length < 4 || elements.length > 5) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        String jobId = elements[1];
        String database = elements[2];
        String importTable = elements[3];
        verifyDCInfo(database, importTable);
        String externalColumnMappingFile;
        if (elements.length == 5) {
            externalColumnMappingFile = elements[4];
            mapReduceOutputImporter.importOutput(jobId, database, importTable, externalColumnMappingFile);
        } else mapReduceOutputImporter.importOutput(jobId, database, importTable);
        return "Data successfully imported";
    }

    /**
     * history of all map-reduce jobs
     * no parameters required as of this moment.
     * TODO: Filter jobs based on input parameters of a job
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String mrJobHistory(String[] elements) throws OperationException {
        return mapReduceJobManager.getHistory().toString();
    }

    /**
     * information about a map-reduce job
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String mrJobInfo(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        StringBuilder sb = new StringBuilder();
        String jobId = elements[1];
        sb.append("Time, Database, InputTable, Mapper, Reducer, OutputTable").append("\n");
        sb.append("Info:\t").append(mapReduceJobManager.getJobInfo(jobId)).append("\n");
        sb.append("Status:\t").append(mapReduceJobManager.getStatus(jobId));
        return sb.toString();
    }

    /**
     * check progress of a map-reduce job
     * [1]: jobId
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String mrJobProgress(String[] elements) throws OperationException {
        return "operation depricated";

//        if (elements.length != 2) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
//        }
//        String jobId = elements[1];
//        return mapReduceExecutor.getProgress(jobId).toString();
    }

    /**
     * status of a map-reduce job
     * [1]: jobId
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String mrJobStatus(String[] elements) throws OperationException {
        return "Operation depricated";

//        if (elements.length != 2) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
//        }
//        return "status: " + mapReduceJobManager.getStatus(elements[1]).toString();
    }

    /**
     * Start a new map-reduce job [1]: database (it must contain the mapper and reducer class inside it) [2]: table from
     * where data is to be read [3]: mapper class [4]: reducer class [5]: output table (optional) where the output of
     * map-reduce job will be imported.
     * <p> As of this moment, we are not supporting the auto importing result into a memory table as soon as the job is finished. </p>
     * TODO: integrate this.
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String mrStartJob(String[] elements) throws OperationException {
        return "Operation depricated";

//        if (elements.length < 5) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
//        }
//        String database = elements[1];
//        String dataTable = elements[2];
//        verifyDCInfo(database, dataTable);
//        String mapperClass = elements[3];
//        String reducerClass = elements[4];
//
//        if (!datastoreManager.exists(database)) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given database doesn't exist");
//        }
//        if (!collectionManager.exists(database, dataTable)) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given table doesn't exist");
//        }
//
//        String outputTable = null;
//        // whether table where data is to be imported is provided or not
//        if (elements.length == 6) {
//            outputTable = elements[5];
//            if (!collectionManager.exists(database, outputTable)) {
//                throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given table " + outputTable + " doesn't exist");
//            }
//        }
//
//        String jobId = mapReduceExecutor.startJob(database, dataTable, mapperClass, reducerClass, outputTable);
//        return "Map-reduce job has been Queued.\nJob Id is: " + jobId;
    }

    /**
     * clear the contents of a in memory table
     * [1]: database.table name
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String clearTable(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        dataManager.clearAllData(database, table);
        return "Table cleared";
    }

    /**
     * Sets the auto define type for a specified column
     * @param elements
     * @return textual confirmation of successful type set
     * @throws OperationException
     */
    private String setAutoDefine(String []elements) throws OperationException {
        if (elements.length != 4) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        final String column = elements[2];
        final AutoDefineTypes autoDefineType = AutoDefineTypes.fromString(elements[3]);

        collectionManager.setAutoDefine(database, table, column, autoDefineType);
        return "Auto define for column " + column + " in " + databaseAndTable + " set to " + autoDefineType.getText();
    }

    /**
     * import a CSV file in a table [1]: database.table [2]: Full path of file
     * <p>
     * It assumes that the column mapping is present in the first line. Also all the columns are assumed as string
     * unless the table already has a schema
     * </p>
     *
     * @param elements
     * @return
     * @throws OperationException
     */
    private String importCSV(String[] elements) throws OperationException {
        long startTime = System.currentTimeMillis();
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }
        final String datastore = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String collection = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        verifyDCInfo(datastore, collection);

        String filePath = elements[2];

        JSONObject opSpecs = new JSONObject();
        opSpecs.put("type", "IMP");
        opSpecs.put("import-type", "CSV");
        opSpecs.put("file", filePath);

        operationsManager.registerOperation(datastore, collection, OperationTypes.IMPORT, opSpecs);

//        mapReduceOutputImporter.importCSVFile(datastore, collection, filePath);

        long elapsedTime = System.currentTimeMillis() - startTime;
        return "Done in " + elapsedTime + " (ms)";
    }

    /**
     * Used to import an Excel document in a specified table. Columns of the table correspond to columns of the Excel
     * and the entry will have one record per row. Sheet name is also stored as a field per record.
     * @param elements query elements from CLI request
     * @return import success/fail status response in text form
     * @throws OperationException if an error occurs while executing the oepration
     */
    private String importExcel(String[] elements) throws OperationException {
        long startTime = System.currentTimeMillis();
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }
        final String datastore = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String collection = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        verifyDCInfo(datastore, collection);

        /* Maybe a public URL or a local file system path. NFS paths are currently not supported */
        String filePath = elements[2];

        JSONObject opSpecs = new JSONObject();
        opSpecs.put("type", "IMP");
        opSpecs.put("import-type", "CSV");
        opSpecs.put("file", filePath);

        operationsManager.registerOperation(datastore, collection, OperationTypes.IMPORT, opSpecs);

//        mapReduceOutputImporter.importCSVFile(datastore, collection, filePath);

        long elapsedTime = System.currentTimeMillis() - startTime;
        return "Done in " + elapsedTime + " (ms)";
    }

    /* Datacube related commands */
    /** these functions should be analysed if they are required and if not then removed **/
    private String createDataCube(String[] elements) throws OperationException {
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }
        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        List<String> cols = new ArrayList<>();
        for (int i = 2; i < elements.length; i++) {
            cols.add(elements[i]);
        }
        dataCubeManager.createDataCube(database, table, cols.toArray(new String[cols.size()]));
        return "Data Cube Created";
    }

    private String deleteDataCube(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        final String cubeName = elements[1];
        dataCubeManager.deleteDataCube(cubeName);
        return "Data cube " + cubeName + " successfully deleted";
    }

    private String renameDataCube(String[] elements) throws OperationException {
        if (elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        dataCubeManager.renameDataCube(elements[1], elements[2]);
        return "Datacube " + elements[1] + " successfully renamed to " + elements[2];
    }


    private String listDataCubes(String elements[]) throws OperationException {
        if (elements.length > 1) {
            String dbOrTable = elements[1];
            if (!datastoreManager.exists(dbOrTable)) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "No such database/table isPresent");
            }
            dataCubeManager.listDataCube(dbOrTable).toString();
        } else {
            return dataCubeManager.listDataCubes().toString();
        }
        return null;
    }

    private String popTable(String[] elements) throws OperationException {

        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        // path of data file
        String filePath = elements[2];
        Boolean schemaInFile = elements[3].equals("true");
        String schemaFile = "";

        if (!schemaInFile) schemaFile = elements[4];
        dataManager.popTable(database, table, filePath, schemaInFile, schemaFile);
        return "Table successfully populated from given file";
    }

    private String populateTable(String[] elements) throws OperationException {
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        if (!MemoryTableStore.exists(databaseAndTable)) {
            return "The specified table does not exist. This operation is only supported for in memory tables";
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        String dataFile = elements[2];
        String header = elements[3];
        boolean headerPresent = (header.equals("true"));
        String columnFile = elements[4];
        dataManager.popTable(database, table, dataFile, headerPresent, columnFile);
        return "Table successfully populated";
    }

    private String repopulateTable(String[] elements) throws OperationException {
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        if (!MemoryTableStore.exists(databaseAndTable)) {
            return "The specified table does not exist. This operation is only supported for in memory tables";
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        String dataFile = elements[2];
        String header = elements[3];
        boolean headerPresent = (header.equals("true"));
        String columnFile = elements[4];
        try {
            dataManager.repopulateTable(database, table, dataFile, headerPresent, columnFile);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(ConsoleExecutorBean.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "Table successfully populated";
    }

    /*  These commands are related to User and User Groups */
    private String createGroup(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        String groupName = elements[1];
        groupManager.createGroup(new UserGroup(groupName));
        return "Group successfully added";
    }

    private String deleteGroup(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        String groupName = elements[1];
        groupManager.deleteGroup(groupName);
        return "Group successfully deleted";
    }

    private String viewGroup(String[] elements) throws OperationException {
        if (elements.length != 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        String groupName = elements[1];
        return groupManager.getGroupInformation(groupName).toString();
    }

    private String renameGroup(String[] elements) throws OperationException {
        if (elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        String oldName = elements[1];
        String newName = elements[2];
        groupManager.renameGroup(oldName, newName);
        return "Group renaming successful";
    }

    private String addUserToGroup(String[] elements) throws OperationException {
        if (elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        String groupName = elements[1];
        String user = elements[2];
        groupManager.addUserToGroup(groupName, user);
        return "User " + user + " added successfully to group: " + groupName;
    }

    private String removeUserFromGroup(String[] elements) throws OperationException {
        if (elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid number of arguments provided");
        }
        String groupName = elements[1];
        String user = elements[2];
        groupManager.removeUserFromGroup(groupName, user);
        return "User " + user + " removed successfully from group: " + groupName;
    }

    /* these commands are used in debugging. Use them at your own risk */
    private String getColumnMapping(String[] elements) throws OperationException {
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());

        JSONObject mapping = schemaStore.getColumnMapping(database, table).toJSONObject();
        return mapping.toString();
    }

    private String getData(String[] elements) throws OperationException {
        if (elements.length < 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String datastore = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String collection = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(datastore, collection);

        int i = 0, cnt = 0;
        /* whether _id is provided or no of rows */
        if (elements.length == 3) {
            try {
                cnt = Integer.valueOf(elements[2]);
            /* catch specific exception here */
            /* it means that an UUID/_id was passed and user wants to get data for that row only */
            } catch (NumberFormatException ex) {
                return dataManager.select(datastore, collection, elements[2]).toString();
            }
        }

        /* select random no of specified rows */
        StringBuilder sb = new StringBuilder();
        Iterator<String> keysItr = dataManager.selectAllKeysAsStream(datastore, collection);
        while (keysItr.hasNext()) {
            sb.append(dataManager.select(datastore, collection, keysItr.next()));
            sb.append("\n");
            if (++i == cnt) break;
        }
        return sb.toString();
    }

    private String getKeys(String[] elements) throws OperationException {
        if (elements.length < 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Database and table should be specified in format: databaseName.tableName");
        }

        final String datastore = databaseAndTable.substring(0, databaseAndTable.indexOf(".", 1)); //start searching dot from index 1 to handle case of .systemdb as datastore name
        final String collection = databaseAndTable.substring(databaseAndTable.indexOf(".", 1) + 1, databaseAndTable.length());
        verifyDCInfo(datastore, collection);

        int i = 0, cnt = 0;
        /* whether no of keys where specified or not */
        if (elements.length == 3) {
            try {
                cnt = Integer.valueOf(elements[2]);
            } catch (NumberFormatException ex) {
                cnt = 0;
            }
        }

        /* select specified no of keys  randomly */
        StringBuilder sb = new StringBuilder();
        Iterator<String> keysItr = dataManager.selectAllKeysAsStream(datastore, collection);
        while (keysItr.hasNext()) {
            sb.append(keysItr.next());
            sb.append("\t");
            i++;
            if (i == cnt) break;
        }
        return sb.toString();
    }

    public String insertCustom(String[] elements) throws OperationException {
        final String datastore = elements[1];
        final String interepreterName = elements[2];
        final String args = elements[3];
        verifyDCInfo(datastore);
        JSONObject out = codeExecutor.executeDataInterpreter(datastore, interepreterName, args);
        return out.toString();
    }

    private String setLogLevel(String[] elements) throws OperationException {
        if (elements.length < 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "set-log-level command");
        }

        /* Ensure no extra parameters are present */
        if (elements.length > 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "set-log-level command takes only a single parameter containing the log level");
        }

        String logLevel = elements[1];

        switch (logLevel.toLowerCase()) {
            case "info":
                LogManager.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
                break;
            case "debug":
                LogManager.getRootLogger().setLevel(org.apache.log4j.Level.DEBUG);
                break;
            case "error":
                LogManager.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
                break;
            case "fatal":
                LogManager.getRootLogger().setLevel(org.apache.log4j.Level.FATAL);
                break;
            case "trace":
                LogManager.getRootLogger().setLevel(org.apache.log4j.Level.TRACE);
                break;
            case "warn":
                LogManager.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
                break;
            case "off":
                LogManager.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
                break;
            case "all":
                LogManager.getRootLogger().setLevel(org.apache.log4j.Level.ALL);
                break;
            default:
                break;
        }

        return "Logging level has been set to " + logLevel.toUpperCase() + " successfully";
    }

    private String rowCount(String []elements) throws OperationException {
        if(elements.length < 2){
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }

        final String datastoreAndCollection = elements[1];
        if (!datastoreAndCollection.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Datastore and collection should be specified in format: datastoreName.collectionName");
        }

        final String datastore = datastoreAndCollection.substring(0, datastoreAndCollection.indexOf("."));
        final String collection = datastoreAndCollection.substring(datastoreAndCollection.indexOf(".") + 1, datastoreAndCollection.length());

        final int count = dataManager.getRowCount(datastore, collection);

        return count + " rows";
    }


    /* Watch service related commands here */

    private String startWatch(String[] elements) throws OperationException {
        // minimum no of arguments (path ds.collection startFromEnd)
        if(elements.length < 3){
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }
        String path = elements[1];
        final String datastoreAndCollection = elements[2];
        if (!datastoreAndCollection.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Datastore and collection should be specified in format: datastoreName.collectionName");
        }

        final String datastore = datastoreAndCollection.substring(0, datastoreAndCollection.indexOf("."));
        final String collection = datastoreAndCollection.substring(datastoreAndCollection.indexOf(".") + 1, datastoreAndCollection.length());
        final WatchServiceImportType importType = elements.length >= 4 ? WatchServiceImportType.fromString(elements[3]) : WatchServiceImportType.LINE;
        final String interpreter = elements.length >= 5 ? elements[4] : null;
        final boolean startFromEnd = elements.length >= 6 ? (elements[5].toLowerCase().equals("from-end") ? true : false) : false;

        watchServiceManager.startWatch(path, datastore, collection, startFromEnd, importType, interpreter);

        return "Watch service activated for " + path;
    }

    private String stopWatch(String[] elements) throws OperationException{
        if(elements.length < 3){
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }
        final String path = elements[1];
        final String ds = elements[2];
        final boolean isDirectory = new File(path).isDirectory();
        if(isDirectory) {
            watchServiceManager.stopWatch(path, ds);
        }
        else {
            watchServiceManager.stopWatch(path, ds);
        }
        return "Success! " +(isDirectory?"Folder ":"File ") + path + " is no longer being watched";
    }

    private String listWatch(String[] elements) throws OperationException{
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Following objects are being watched: " + "\n");
        stringBuilder.append("Folders:\n");
        stringBuilder.append(watchServiceManager.listFolderWatches());
        stringBuilder.append("\n");
        stringBuilder.append("Files:\n");
        stringBuilder.append(watchServiceManager.listFileWatches());
        stringBuilder.append("\n");
        stringBuilder.append("Folder-Files:\n");
        stringBuilder.append(watchServiceManager.listFolderWatchJSON());
        return stringBuilder.toString();
    }

    private String setReplication(String []elements) throws OperationException {
        if(elements.length < 3){
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Insufficient number of arguments provided");
        }
        final String datastoreAndOrCollection = elements[1];
        String datastore;
        String collection;
        if (datastoreAndOrCollection.contains(".")) {
            datastore = datastoreAndOrCollection.substring(0, datastoreAndOrCollection.indexOf("."));
            collection = datastoreAndOrCollection.substring(datastoreAndOrCollection.indexOf(".") + 1, datastoreAndOrCollection.length());
        } else {
            datastore = datastoreAndOrCollection;
            collection = null;
        }

        final ReplicationType replicationType = ReplicationType.fromString(elements[2]);
        final int replicationFactor = (replicationType == ReplicationType.DISTRIBUTED && elements.length >= 4) ? Integer.parseInt(elements[3]) : 0;

        if(collection == null) {
            if(!datastoreManager.exists(datastore)) {
                throw new OperationException(ErrorCode.DATASTORE_INVALID);
            }
            collectionManager.setReplication(datastore, replicationType, replicationFactor);
        } else {
            if(!collectionManager.exists(datastore, collection)) {
                throw new OperationException(ErrorCode.COLLECTION_INVALID);
            }
            collectionManager.setReplication(datastore, collection, replicationType, replicationFactor);
        }

        return "Replication update successful";
    }

    private String setGeoReplication(String []elements) {
        return "Function not supported yet";
    }


    private String tableau(String []elements) throws OperationException {
        if(elements.length < 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "tableau command requires a minimum of 2 parameters");
        }

        TableauCommands command = TableauCommands.fromCode(elements[1].toLowerCase());

        if(command == null) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, elements[1] + " not a valid sub-command");
        }

        switch(command) {
            case PUBLISH:
                return processTableauPublish(elements);
            case AUTO_PUBLISH:
                return processTableauAutoPublish(elements);
        }

        throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT);
    }

    private String processTableauPublish(String []elements) throws OperationException {
        if(elements.length != 3) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT,"tableau publish command requires exactly 3 parameters");
        }

        final String datastoreAndOrCollection = elements[2];
        String datastore;
        String collection;
        if (datastoreAndOrCollection.contains(".")) {
            datastore = datastoreAndOrCollection.substring(0, datastoreAndOrCollection.indexOf("."));
            collection = datastoreAndOrCollection.substring(datastoreAndOrCollection.indexOf(".") + 1, datastoreAndOrCollection.length());
        } else {
            datastore = datastoreAndOrCollection;
            collection = null;
        }

        if(collection == null) {
            tableauTdeManager.createAndPublishTdes(datastore);
        } else {
            tableauTdeManager.createAndPublishTde(datastore, collection);
        }

        return "Data successfully published";
    }

    private String processTableauAutoPublish(String []elements) throws OperationException {
        if(elements.length != 4) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT,"tableau auto-publish command requires exactly 4 parameters");
        }

        final String datastoreAndOrCollection = elements[3];
        String datastore;
        String collection;
        if (datastoreAndOrCollection.contains(".")) {
            datastore = datastoreAndOrCollection.substring(0, datastoreAndOrCollection.indexOf("."));
            collection = datastoreAndOrCollection.substring(datastoreAndOrCollection.indexOf(".") + 1, datastoreAndOrCollection.length());
        } else {
            datastore = datastoreAndOrCollection;
            collection = null;
        }

        if(elements[2].equalsIgnoreCase("on")) {
            if(collection == null) {
                tableauPublishManager.setAutoPublishOn(datastore);
            } else {
                tableauPublishManager.setAutoPublishOn(datastore, collection);
            }
        } else if(elements[2].equalsIgnoreCase("off")) {
            if(collection == null) {
                tableauPublishManager.setAutoPublishOff(datastore);
            } else {
                tableauPublishManager.setAutoPublishOff(datastore, collection);
            }
        } else {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "tableau auto-publish state should be 'on' or 'off', but found " + elements[2]);
        }

        return "tableau auto-publish set";
    }

    private String startDsFtp(final String []elements) throws OperationException {
        if(elements.length < 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing required parameter datastore name");
        }

        final String datastore = elements[1];

        return "FTP password is: " + ftpServiceManager.startFtpService(datastore);
    }

    private String stopDsFtp(final String []elements) throws OperationException {
        if(elements.length < 2) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing required parameter datastore name");
        }

        final String datastore = elements[1];

        ftpServiceManager.stopFtpService(datastore);

        return "FTP service stopped for datastore: " + datastore;
    }

    private String setInterpreters(final String []elements) throws OperationException {
        return "Operation not supported yet";
    }

    private String setColumnType(final String []elements) throws OperationException {
        if(elements.length < 4) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing arguments. Required format: set-column-type <ds>.<collection> <column-name> <datatype>");
        }

        final String datastoreAndOrCollection = elements[1];
        String datastore;
        String collection;
        if (datastoreAndOrCollection.contains(".")) {
            datastore = datastoreAndOrCollection.substring(0, datastoreAndOrCollection.indexOf("."));
            collection = datastoreAndOrCollection.substring(datastoreAndOrCollection.indexOf(".") + 1, datastoreAndOrCollection.length());
        } else {
            datastore = datastoreAndOrCollection;
            collection = null;
        }

        final String columnName = elements[2];
        final FieldType fieldType = FieldTypeFactory.fromString(elements[3]);

        collectionManager.changeDataType(datastore, collection, columnName, fieldType);

        return "Column type successfully updated in schema";
    }

    private String createMasterKey() throws OperationException {
        return securityManager.createMasterKey();
    }

    private String createDsKey(final String []elements) throws OperationException {
        if(elements.length != 2) {
            return "Required format: create-ds-key {ds}";
        }
        final String ds = elements[1];
        return securityManager.createDsKey(ds);
    }

    private String listApiKeys() throws OperationException {
        return String.join("\n", securityManager.getApiKeys());
    }

    private String listDsApiKeys(final String []elements) throws OperationException {
        if(elements.length != 2) {
            return "Required format: create-ds-key {ds}";
        }
        final String ds = elements[1];
        return String.join("\n", securityManager.getDsApiKeys(ds));
    }

    private String dropApiKey(final String []elements) throws OperationException {
        if(elements.length != 2) {
            return "Required format: drop-api-key {key}";
        }

        securityManager.dropApiKey(elements[1]);
        return "API key successfully dropped";
    }
}
