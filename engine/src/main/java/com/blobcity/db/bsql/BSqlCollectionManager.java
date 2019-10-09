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
import com.blobcity.db.data.RowCountManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.indexcache.OnDiskBtreeIndexCache;
import com.blobcity.db.indexing.IndexFactory;
import com.blobcity.db.indexing.IndexingStrategy;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import com.blobcity.db.memory.collection.MemCollection;
import com.blobcity.db.memory.collection.MemCollectionStoreBean;
import com.blobcity.db.memory.old.MemoryTableStore;
import com.blobcity.db.operations.OperationLogLevel;
import com.blobcity.db.schema.*;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.sql.util.PathUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * <p>
 * This class gives functions for managing a BSqlTable. The management operations currently supported by this class are:</p>
 *
 * <p>
 * <li>Create a new table</li>
 * <li>Drop an existing table</li>
 * <li>Rename an existing table</li>
 * <li>Truncate an existing table</li></p>
 *
 * @author sanketsarang
 */
@Component
public class BSqlCollectionManager {
    private static final Logger logger = LoggerFactory.getLogger(BSqlCollectionManager.class.getName());
    
    @Autowired
    private SchemaManager schemaManager;
    @Autowired
    private IndexFactory indexFactory;
    @Autowired
    private BSqlIndexManager indexManager;
    @Autowired
    private RowCountManager rowCountManager;
    @Autowired
    private SchemaStore schemaStore;
    @Autowired
    private MemCollectionStoreBean memCollectionStoreBean;
    @Autowired @Lazy
    private OnDiskBtreeIndexCache onDiskBtreeIndexCache;
    
    /**
     * Create a new table with the given name
     *
     * @param datastore
     * @param collection The name of the table to create
     * @throws OperationException if the table name string is not confirming to a valid SQL table name, or a table with the given name is already present or the
     * create table operation fails to execute on the file system or if the {@code account} and {@code database} fields are not set for this object.
     */
    @Deprecated
    public void createTable(final String datastore, final String collection) throws OperationException {
        JSONObject schemaJson = new JSONObject();
        JSONObject metaJson = new JSONObject();
        
        /* Default meta properties of a table when no specific property is specified */
        metaJson.put(SchemaProperties.TABLE_TYPE, TableType.ON_DISK.getType());
        metaJson.put(SchemaProperties.FLEXIBLE_SCHEMA, true);
        metaJson.put(SchemaProperties.REPLICATION_TYPE, ReplicationType.DISTRIBUTED.getType());
        metaJson.put(SchemaProperties.REPLICATION_FACTOR, 0);
        
        schemaJson.put(SchemaProperties.META, metaJson);
        createTable(datastore, collection, schemaJson);
    }

    /**
     * Create a new table with the given name
     *
     * @param datastore
     * @param collection The name of the table to create
     * @throws OperationException if the table name string is not confirming to a valid SQL table name, or a table with the given name is already present or the
     * create table operation fails to execute on the file system or if the {@code account} and {@code database} fields are not set for this object.
     */
    public void createTable(final String datastore, final String collection, TableType tableType, ReplicationType replicationType, Integer replicationFactor) throws OperationException {
//        /* Perform a license check */
//        if((!LicenseRules.MEMORY_DURABLE_TABLES && tableType == TableType.IN_MEMORY)
//                || (!LicenseRules.MEMORY_NON_DURABLE_TABLES && tableType == TableType.IN_MEMORY_NON_DURABLE)) {
//            throw new OperationException(ErrorCode.FEATURE_RESTRICTED, "Current license does not allow in-memory storage");
//        }

        JSONObject schemaJson = new JSONObject();
        JSONObject metaJson = new JSONObject();

        /* Default meta properties of a table when no specific property is specified */
        metaJson.put(SchemaProperties.TABLE_TYPE, tableType.getType());
        metaJson.put(SchemaProperties.FLEXIBLE_SCHEMA, true);
        metaJson.put(SchemaProperties.REPLICATION_TYPE, replicationType.getType());
        if(replicationType == ReplicationType.DISTRIBUTED) {
            metaJson.put(SchemaProperties.REPLICATION_FACTOR, replicationFactor);
        }

        schemaJson.put(SchemaProperties.META, metaJson);
        createTable(datastore, collection, schemaJson);
    }

    public void createTable(final String datastore, final String collection, final JSONObject schemaJson) throws OperationException {
        String absolutePath;
        File file;

        /* Check whether required credentails are set */
        if (datastore == null || datastore.isEmpty()) {
            logger.warn("Invalid app/db name");
            throw new OperationException(ErrorCode.APP_INVALID, "Required credentails not set when trying to check if a table isPresent");
        }

        if (!new File(PathUtil.databaseFolder(datastore)).exists()) {
            logger.warn("Cannot create table in an in-existent datastore. ds = {}", new Object[]{datastore});
            throw new OperationException(ErrorCode.DATASTORE_INVALID, "Cannot create table in an in-existent datastore. ds = " + datastore);
        }

        /* Check whether table with given name is already present */
        absolutePath = PathUtil.tableFolderPath(datastore, collection);
        file = new File(absolutePath);
        if (file.exists()) {
            logger.warn("Cannot create table as table with given name already isPresent. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.DUPLICATE_COLLECTION_NAME, "Cannot create table. A table with "
                    + "the given name: " + collection + " already isPresent");
        }

        /* Make table directory */
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Create all required folders that are internal to the table */
        createTableArtifacts(datastore, collection);

        try {
            final Schema schema = new Schema(schemaJson);
            schemaManager.writeSchema(datastore, collection, schema);
            final ColumnMapping mapping = SchemaStore.getInstance().getColumnMapping(datastore, collection);
            
            // add a new primary column which is autodefined if one does not exist already
            if(schema.getPrimary() == null) {
                schemaManager.insertPrimaryColumn(datastore, collection);
                /* If you need to add a primary column, use one in the schema manager only. Better for code stabililty */
//                addColumn(dsSet, collection, "_id", FieldTypeFactory.fromString("string"), AutoDefineTypes.UUID, IndexTypes.UNIQUE);
            }

            TableType t = schema.getTableType();
//            MemoryTable memTbl = null;
            if((t == TableType.IN_MEMORY) || (t == TableType.IN_MEMORY_NON_DURABLE)) {
                String tableName = datastore + "." + collection;
                memCollectionStoreBean.add(tableName, new MemCollection(tableName));
//                MemoryTableStore.add(tableName);
//                memTbl = MemoryTableStore.getTable(tableName);
            }
            /* Run initializeIndexing generation on columns that are indexed. This is mainly used to simply create the initializeIndexing folder.
             * In some cases if table creation was fired on an existing data set, then the initializeIndexing operation will
             * run to initializeIndexing the records.
             */
            for (String columnName : schema.getColumnMap().keySet()) {
                if (columnName.equals(schema.getPrimary())) {
                    continue;
                }
                
                final Column column = schema.getColumn(columnName);
                if (column.getIndexType() != IndexTypes.NONE) {
                    IndexingStrategy indexingStrategy = indexFactory.getStrategy(column.getIndexType());
                    indexingStrategy.initializeIndexing(datastore, collection, columnName);
                }
                // create the data cube and columns in data cube for in memory tables
//                if (memTbl != null) {
//                    final FieldType colType = column.getFieldType();
//                    String tableName = datastore + "." + collection;
//                    memTbl.getDataCube().createColumn(columnName, tableName);
//                    memTbl.getDataCube().getColumn(columnName).setColType(colType.getType());
//                    memTbl.getDataCube().getColumn(columnName).setColInternalName(mapping.getInternalName(columnName));
//                    memTbl.getDataCube().getColumn(columnName).setColViewableName(mapping.getViewableName(columnName));
//                }
            }
        } catch (JSONException ex) {
            undoCreateTable(datastore, collection);
            logger.warn("JSON exception with creating table. app = " + datastore + ". table = " + collection, ex);
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "Invalid json schema");
        } catch (OperationException ex) {
            undoCreateTable(datastore, collection);
            throw ex;
        }
    }

    /**
     * Drops an existing table
     *
     * @param datastore
     * @param collection The name of the table to drop
     * @throws OperationException if a table with the given name is not found or if the table delete operation fails
     * on the file system or if the {@code account} and {@code database} fields are not set for this object.
     */
    public void dropTable(final String datastore, final String collection) throws OperationException {

        String absolutePath;
        String backupPath;
        File file;

        /* Check whether required credentails are set */
        if (datastore == null || datastore.isEmpty()) {
            throw new OperationException(ErrorCode.APP_INVALID, "Required credentails not set, when trying to perform drop table operation");
        }

        /* initialize variables */
        absolutePath = PathUtil.tableFolderPath(datastore, collection);
        file = new File(absolutePath);

        /* Check whether table isPresent */
        if (!file.exists()) {
            throw new OperationException(ErrorCode.COLLECTION_INVALID, "Attempting to drop an inexistent table");
        }

        /* Move table to delete folder */
        long currentTime = System.currentTimeMillis();
        backupPath = BSql.BSQL_BASE_FOLDER + datastore + BSql.DELETE_FOLDER + collection + "." + currentTime;
        try {
            int count = 0;
            while (Files.exists(FileSystems.getDefault().getPath(backupPath))) {
                backupPath = BSql.BSQL_BASE_FOLDER + datastore + BSql.DELETE_FOLDER + collection + "." + currentTime + "_" + count;
                count++;
            }
            Files.move(FileSystems.getDefault().getPath(absolutePath), FileSystems.getDefault().getPath(backupPath), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {

            //TODO: Notify Admin
            throw new OperationException(ErrorCode.COLLECTION_DELETION_ERROR, "Table could not be dropped as move to backup folder failed.");
        }
        // remove schema from the memory
        schemaStore.invalidateSchema(datastore, collection);
        
        String tableName = datastore + "." + collection;
        if(MemoryTableStore.exists(tableName)) {
            MemoryTableStore.delete(tableName);
            // TODO: delete data cube from here
        }

        /* Invalidate any index caches */
        onDiskBtreeIndexCache.invalidate(datastore, collection);
    }

    /**
     * Drops an existing collection without archive
     *
     * @param datastore name of datastore
     * @param collection name of collection
     * @throws OperationException if the drop operation fails
     */
    public void dropCollectionNoArchive(final String datastore, final String collection) throws OperationException {

        String absolutePath;
        String backupPath;
        File file;

        /* Check whether required credentails are set */
        if (datastore == null || datastore.isEmpty()) {
            throw new OperationException(ErrorCode.APP_INVALID, "Required credentails not set, when trying to perform drop collection operation");
        }

        /* initialize variables */
        absolutePath = PathUtil.tableFolderPath(datastore, collection);
        file = new File(absolutePath);

        try{
            Files.delete(FileSystems.getDefault().getPath(absolutePath));
        } catch (IOException ex) {

            //TODO: Notify Admin
            throw new OperationException(ErrorCode.COLLECTION_DELETION_ERROR, "Collection could not be dropped due to a disk error");
        }
        // remove schema from the memory
        schemaStore.invalidateSchema(datastore, collection);

        String tableName = datastore + "." + collection;
        if(MemoryTableStore.exists(tableName)) {
            MemoryTableStore.delete(tableName);
            // TODO: delete data cube from here
        }

        /* Invalidate any index caches */
        onDiskBtreeIndexCache.invalidate(datastore, collection);
    }

    public void undoDropCollection(final String archiveCode) throws OperationException {

        //TODO: Implement this

        throw new UnsupportedOperationException("Not supported yet");
    }

    /**
     * Deletes all the data present in an existing table (schema is not deleted)
     *
     * @param datastore
     * @param collection The name of the table to truncate
     * @throws OperationException if a table with the given name is not found or if the table truncate operation failes on the file system or if the
     * {@code account} and {@code database} fields are not set for this object.
     */
    public void truncateTable(final String datastore, final String collection) throws OperationException {
        Schema schema = schemaManager.readSchema(datastore, collection);
        JSONObject jsonSchema;
        try {
            jsonSchema = schema.toJSONObject();
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. Your table is not truncated.");
        }
        dropTable(datastore, collection);
        createTable(datastore, collection, jsonSchema);

        /* Invalidate any index caches */
        onDiskBtreeIndexCache.invalidate(datastore, collection);
    }

    /**
     * Renames an existing table in the database
     *
     * @param datastore The application id of the BlobCity application
     * @param collectionOldName The existing name of the table which is to be renamed
     * @param collectionNewName The new name of the table to which the table is to be renamed to
     * @throws OperationException if table with name equal to {@code tableName} is not found, or if {@code newName} does not confirm to a valid SQL table name
     * format, or if a table with name same as {@code newName} is already existing or if the {@code account} and {@code database} fields are not set for this
     * object.
     */
    public void renameTable(final String datastore, String collectionOldName, String collectionNewName) throws OperationException {
        String currentTablePath;
        String newTablePath;
        File currentTableFile;
        File newTableFile;

        /* Check whether required credentails are set */
        if (datastore == null || datastore.isEmpty()) {
            throw new OperationException(ErrorCode.APP_INVALID, "Required credentails not set when trying to check if a table isPresent");
        }

        /* Initialize variables */
        currentTablePath = PathUtil.tableFolderPath(datastore, collectionOldName);
        newTablePath = PathUtil.tableFolderPath(datastore, collectionNewName);
        currentTableFile = new File(currentTablePath);
        newTableFile = new File(newTablePath);

        /* Check whether current table isPresent */
        if (!currentTableFile.exists()) {
            throw new OperationException(ErrorCode.COLLECTION_INVALID, "Attempting to renmae table: "
                    + collectionOldName + ", but no table with the given name found");
        }

        /* Check whether a table with the new name isPresent */
        if (newTableFile.exists()) {
            throw new OperationException(ErrorCode.DUPLICATE_COLLECTION_NAME, "Attempting to rename table: "
                    + collectionOldName + " to table: " + collectionNewName
                    + ", but a table with the name " + collectionNewName + " already isPresent");
        }

        if (!currentTableFile.renameTo(newTableFile)) {
            throw new OperationException(ErrorCode.RENAME_COLLECTION_ERROR, "Rename of table: " + collectionOldName + " to table: " + collectionNewName
                    + " failed with a file system error. Please contact system administrators");
        }
        
        String oldTableName = datastore + "." + collectionOldName;
        if(MemoryTableStore.exists(oldTableName)) {
            String newTableName = datastore + "." + collectionNewName;
            MemoryTableStore.rename(oldTableName, newTableName);
            // TODO: update data cube here with column rename
        }

        /* Invalidate any index caches */
        onDiskBtreeIndexCache.invalidate(datastore, collectionOldName);
        onDiskBtreeIndexCache.invalidate(datastore, collectionNewName); //just in case someone screws up the code
    }

    /**
     * Used for checking if a collection is present
     *
     * @param ds name of datastore
     * @param collection name of collection
     * @return true if collection is existent, false otherwise
     */
    public boolean exists(final String ds, final String collection) {
        if (ds == null || ds.isEmpty() || collection == null || collection.isEmpty()) {
            return false;
        }
        File file = new File(PathUtil.tableFolderPath(ds, collection));
        return file.exists();
    }

    public List<String> listTables(final String datastore) throws OperationException {
        File file;
        List<String> tableNames;

        /* Check whether all required credentails are set */
        if (datastore == null || datastore.isEmpty()) {
            throw new OperationException(ErrorCode.APP_INVALID, "Required credentails not set when trying to perform list tables operation");
        }

        file = new File(PathUtil.databaseFolder(datastore));

        final File[] tableFiles = file.listFiles((File pathname) -> pathname.isDirectory() && new File(pathname.getAbsoluteFile() + "/data").exists());

        if (tableFiles.length == 0) {
            return Collections.EMPTY_LIST;
        }

        tableNames = new ArrayList<>();
        for (File tableFile : tableFiles) {
            tableNames.add(tableFile.getName());
        }

        return tableNames;
    }

    /**
     * <p>
     * Adds a column of the given type to the table</p>
     *
     * @param datastore
     * @param collection
     * @param columnName The name of the column to add
     * @param dataType The datatype of the column to add
     * @param autoNumberedType The auto numbering type for the column, NONE if column is not auto numbered
     * @param indexType The initializeIndexing type of the column, NONE if the column is not indexed
     * @throws OperationException if a column with the existing name is already preset, or if the column name is an invalid column or the datatype is an invalid
     * data type or the revised schema after column addition does not adhere to schema rule sets
     */
    public void addColumn(final String datastore, final String collection, final String columnName, final FieldType dataType, final AutoDefineTypes autoNumberedType, final IndexTypes indexType) throws OperationException {
        Schema schema = null;
        try {
            schema = schemaManager.readSchema(datastore, collection);
        } catch (OperationException ex) {
            if (ex.getErrorCode() == ErrorCode.SCHEMA_FILE_NOT_FOUND) {
                schema = new Schema();
                schema.setPrimary(columnName);
            } else {
                throw ex;
            }
        }

        /* Check if column with same name already isPresent */
        if (schema.getColumnMap().containsKey(columnName)) {
            throw new OperationException(ErrorCode.DUPLICATE_COLUMN_NAME, "Attempting to add column: " + columnName
                    + " to table: " + collection + ", but a column with the same name already isPresent");
        }

        Column column = new Column(columnName, dataType, indexType, autoNumberedType);
        schema.getColumnMap().put(columnName, column);

        /* Used to set primary key field when first column is added. The first column is defaulted to primary key */
        if (schema.getColumnMap().size() == 1 && (schema.getPrimary() == null || schema.getPrimary().isEmpty())) {
            schema.setPrimary(columnName);
        }

        try {
            schemaManager.writeSchema(datastore, collection, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
        // TODO: Update datacube and add a new column from here        
        /* Index records for the specified column if the column requires indexing */
        if (indexType != IndexTypes.NONE) {
            indexManager.indexRecords(datastore, collection, columnName, OperationLogLevel.ERROR);
        }
    }

    public void dropColumn(final String datastore, final String collection, final String columnName) throws OperationException {
        final Schema schema = schemaManager.readSchema(datastore, collection);

        /* Check if column with same name already isPresent */
        if (!schema.getColumnMap().containsKey(columnName)) {
            throw new OperationException(ErrorCode.COLUMN_INVALID, "Attempting to remove column: " + columnName
                    + " from table: " + collection + ", but no column was found with the specified name");
        }

        /* Restrict deletion of primary key */
        if (schema.getPrimary().equals(columnName)) {
            throw new OperationException(ErrorCode.DROP_COLUMN_ERROR, "Attempting to delete primary key column: " + columnName
                    + ", a primary key column can only be altered not deleted.");
        }

        schema.getColumnMap().remove(columnName);
        try {
            schemaManager.writeSchema(datastore, collection, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        /* Invalidate index cache for the column */
        onDiskBtreeIndexCache.invalidate(datastore, collection, columnName);
    }

    public void renameColumn(final String datastore, final String collection, final String existingName, final String newName) throws OperationException {
        final Schema schema = schemaManager.readSchema(datastore, collection);
        final ColumnMapping columnMapping = schemaManager.readColumnMapping(datastore, collection);

        /* Check if column with same name already isPresent */
        if (!schema.getColumnMap().containsKey(existingName)) {
            throw new OperationException(ErrorCode.COLUMN_INVALID, "Attempting to rename column: " + existingName
                    + " from table: " + collection + ", but no column was found with the specified name");
        }

        /* The column mapping file must also have a mapping for the specified column */
        if (!columnMapping.getViewableNameMap().containsKey(existingName)) {
            throw new OperationException(ErrorCode.COLUMN_INVALID, "Attempting to rename column: " + existingName
                    + " from table: " + collection + ", but internal column mapping schema file does not have an entry for the column");
        }

        if (schema.getColumnMap().containsKey(newName)) {
            throw new OperationException(ErrorCode.DUPLICATE_COLUMN_NAME, "Attempting to rename column: " + existingName
                    + " from table: " + collection + " to column: " + newName + " but another column already isPresent with the name: " + newName);
        }

        try {
            final Column column = schema.getColumnMap().get(existingName);
            schema.getColumnMap().remove(existingName);
            column.setName(newName);
            schema.getColumnMap().put(newName, column);
            if (schema.getPrimary().equals(existingName)) {
                schema.setPrimary(newName);
            }
            columnMapping.renameMappedColumn(existingName, newName);

            /* The new column mapping needs to be written first as schma writer auto syncs columns mapping */
            schemaManager.writeColumnMapping(datastore, collection, columnMapping);
            schemaManager.writeSchema(datastore, collection, schema, true);
            //TODO: update DATACUBE
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        /* Invalidate index cache for the column */
        onDiskBtreeIndexCache.invalidate(datastore, collection, existingName);
        onDiskBtreeIndexCache.invalidate(datastore, collection, newName); //just in case someone screws up the code
    }

    /**
     * Changes the data type and auto define type of the specified column. A value of null for either data type or auto define type indicates that the
     * respective property of the column is not to be changed.
     *
     * @param datastore The application id of the BlobCity application
     * @param collection The name of the table
     * @param columnName The name of the column to alter
     * @param dataType The new data type of the column from {@link DataTypes}, null if existing data type is to be retained
     * @param autoDefineTypes The new auto define type of the column from {@link AutoDefineTypes}, null if the existing auto define type is to be retained
     * @throws OperationException for reporting any errors that occur while performing this operation along with respective error codes
     */
    public void alterColumn(final String datastore, final String collection, final String columnName, final FieldType dataType, final AutoDefineTypes autoDefineTypes) throws OperationException {
        final Schema schema = schemaManager.readSchema(datastore, collection);

        /* Check if column with same name already isPresent */
        if (!schema.getColumnMap().containsKey(columnName)) {
            throw new OperationException(ErrorCode.COLUMN_INVALID, "Attempting to alter column: " + columnName
                    + " from table: " + collection + ", but no column was found with the specified name");
        }

        if (dataType != null) {
            schema.getColumnMap().get(columnName).setFieldType(dataType);
        }

        if (autoDefineTypes != null) {
            schema.getColumnMap().get(columnName).setAutoDefineType(autoDefineTypes);
        }

        try {
            schemaManager.writeSchema(datastore, collection, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        /* The index structure and contents can change, hence invalidate the index cache */
        onDiskBtreeIndexCache.invalidate(datastore, collection, columnName);
    }

    public void changeDataType(final String datastore, final String collection, final String columnName, final FieldType newDataType) throws OperationException {
        final Schema schema = schemaManager.readSchema(datastore, collection);

        /* Check if column with same name already isPresent */
        if (!schema.getColumnMap().containsKey(columnName)) {
            throw new OperationException(ErrorCode.COLUMN_INVALID, "Attempting to alter column: " + columnName
                    + " from table: " + collection + ", but no column was found with the specified name");
        }

        schema.getColumnMap().get(columnName).setFieldType(newDataType);
        try {
            schemaManager.writeSchema(datastore, collection, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        /* The index structure and contents can change, hence invalidate the index cache */
        onDiskBtreeIndexCache.invalidate(datastore, collection, columnName);
    }

    public void setAutoDefine(final String datastore, final String collection, final String columnName, final AutoDefineTypes autoDefineType) throws OperationException {
        final Schema schema = schemaManager.readSchema(datastore, collection);

        /* Check if column with same name already isPresent */
        if (!schema.getColumnMap().containsKey(columnName)) {
            throw new OperationException(ErrorCode.COLUMN_INVALID, "Attempting to alter column: " + columnName
                    + " from table: " + collection + ", but no column was found with the specified name");
        }

        schema.getColumnMap().get(columnName).setAutoDefineType(autoDefineType);
        try {
            schemaManager.writeSchema(datastore, collection, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        /* The index structure and contents can change, hence invalidate the index cache */
        onDiskBtreeIndexCache.invalidate(datastore, collection, columnName);
    }

    /**
     * Creates all required artifacts within the table folder. A table is marked by a folder and internally has many folders and files that organization the
     * storage structure within the table. This function assumes that the main table folder is already created and creates the required folders and files within
     * the table folder. This function is not aware of schema specific artifacts and creates the pre-requisite folders and files only which are independent of
     * the table schema.
     *
     * @param datastore the application id of the BlobCity application
     * @param collection name of table within the application
     * @throws OperationException if a file system or I/O error occurs while creating the folders
     */
    private void createTableArtifacts(final String datastore, final String collection) throws OperationException {
        File file;
        final String absolutePath = PathUtil.tableFolderPath(datastore, collection);

        /* Make table data directory */
        file = new File(absolutePath + "/data");
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table data folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Make table meta directory */
        file = new File(absolutePath + "/meta");
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table meta folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Make table indexing directory */
        file = new File(absolutePath + "/index");
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table index folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Make table index count directory */
        file = new File(absolutePath + "/index-count");
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table index count folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Make table operation directory */
        file = new File(absolutePath + "/ops");
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table operations folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Make table import file storage directory */
        file = new File(absolutePath + "/import");
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table import folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Make table export file storage directory */
        file = new File(absolutePath + "/export");
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table export folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Make commit logs directory */
        file = new File(absolutePath + "/commit-logs");
        if (!file.mkdir()) {
            undoCreateTable(datastore, collection);
            logger.warn("Could not create table commit-logs folder. app = {}. table = {}", new Object[]{datastore, collection});
            throw new OperationException(ErrorCode.COLLECTION_CREATION_ERROR, "The table: " + collection + " could not be created");
        }

        /* Make row count file with entry for zero records as table is new  */
        rowCountManager.writeCount(datastore, collection, 0);
    }

    /**
     * Deletes all files and folders present inside a directory object
     *
     * @param directory A {@link File} object which points to a directory who's all contents are to be deleted
     */
    private void deleteAllContainedFiles(final String datastore, final String collection) throws OperationException {
        Iterator<Path> pathIterator = getTableFolderAsStream(datastore, collection);
        while (pathIterator.hasNext()) {
            if (!pathIterator.next().toFile().delete()) {
                throw new OperationException(ErrorCode.INADEQUATE_FILE_SYSTEM_PERMISSION);
            }
        }
    }

    private Iterator<Path> getTableFolderAsStream(final String datastore, final String collection) {
        try {
            return Files.newDirectoryStream(FileSystems.getDefault().getPath(PathUtil.tableFolderPath(datastore, collection))).iterator();
        } catch (IOException ex) {
            logger.error(null, ex);
        }
        return null;
    }

    private void undoCreateTable(final String datastore, final String collection) {
        String absolutePath;

        /* Check whether required credentails are set */
        if (datastore == null || datastore.isEmpty()) {
            return;
        }

        absolutePath = PathUtil.tableFolderPath(datastore, collection);
        deleteWithContents(new File(absolutePath));
    }

    private void deleteWithContents(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteWithContents(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
    
    /**
     * checks whether a given table is in memory or not.
     * if not present in memory store, then it will add it also. 
     * Data is also imported for durable tables. (TODO in future)
     * 
     * @param datastore
     * @param collection
     * @return
     * @throws OperationException 
     */
    public boolean isInMemory(final String datastore, final String collection) throws OperationException{
        
        // check if already added to memory store
        if(MemoryTableStore.exists(datastore+"."+collection)) return true;
        // check if table is present or not on disk (structure only)
        if(!exists(datastore, collection)) return false;
        
        //Only done on database boot ups
        //read schema and check type
        Schema schema = schemaManager.readSchema(datastore, collection);
        if(schema.getTableType().equals(TableType.IN_MEMORY)){
            if(!MemoryTableStore.exists(datastore+"."+collection))
                    MemoryTableStore.add(datastore+"."+collection);
            // TODO: put data back in memory here if is durable
            return true;
        }
        else if( schema.getTableType().equals(TableType.IN_MEMORY_NON_DURABLE)){
            if(!MemoryTableStore.exists(datastore+"."+collection))
                    MemoryTableStore.add(datastore+"."+collection);
            return true;
        }
        return false;
    }

    /**
     * Add a new column in the database based on auto-inferring the column Type
     * Not Done currently. Should be done in Future
     *
     * @param datastore
     * @param collection
     * @param columnName
     */
    public void addInferedColumn(final String datastore, final String collection, final String columnName) throws OperationException{
        addColumn(datastore, collection, columnName, FieldTypeFactory.fromString("String"), AutoDefineTypes.NONE, IndexTypes.NONE);

        /* The index structure and contents can change, hence invalidate the index cache */
        onDiskBtreeIndexCache.invalidate(datastore, collection, columnName);
    }

    public void setReplication(final String datastore, final ReplicationType replicationType, final int replicationFactor) throws OperationException {
        List<String> collectionList = listTables(datastore);

        for(String collection : collectionList) {
            setReplication(datastore, collection, replicationType, replicationFactor);
        }
    }

    public void setReplication(final String datastore, final String collection, final ReplicationType replicationType, final int replicationFactor) throws OperationException {
        Schema schema = schemaStore.getSchema(datastore, collection);
        schema.setReplicationType(replicationType);
        schema.setReplicationFactor(replicationFactor);
        schemaManager.writeSchema(datastore, collection, schema, true);

        //TODO: Start appropriate data sync operation in background
    }

}
