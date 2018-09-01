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

package com.blobcity.db.versioning;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.operations.OperationLogLevel;
import com.blobcity.db.schema.*;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.storage.BSqlFileManager;
import com.blobcity.db.util.FileNameEncoding;
import com.blobcity.db.util.SystemInputUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is used to upgrade to version 1.4 or above from version 1.3
 * 
 * Things to take care of in this version:
 * 1. userGroups table creation on startup of database
 * 2. convert each table into flexible schema table with primary id autoDefined as uuid here
 *
 * @author sanketsarang
 */
public class Version3to4 implements VersionUpgrader{
    
    private static final Logger logger = LoggerFactory.getLogger(Version3to4.class);
    
    private static final String OLD_VERSION = "3";
    private static final String NEW_VERSION = "4";
    private static final String BACKUP_FILE_POSTFIX = "-v3.zip";
        
    // folders to skip in upgrading.
    private final Set<String> skipAppLevelFolders = new HashSet<>(Arrays.asList(new String[]{"global-live", "global-del", "BlobCityDB"}));

    @Override
    public void upgrade() {
        logger.info("Upgrading from " + OLD_VERSION + " to new version " + NEW_VERSION + " . . . ");
        try{
            //TODO: erorr in creating zip of the folder.
            // needs to be  checked
//            performBackup();
            
            startUpgrade();
            
//            removeBackup();
        } catch(Exception ex){
            logger.warn("Data storage format upgrade to version " + NEW_VERSION + " failed. The application may not function correctly.");
            logger.error(null, ex);
        }
    }
    
    public void performBackup(){
        try {
            logger.info("Archiving current data for backup");
            System.out.print("Zipping .");
            final String baseFolder = BSql.BSQL_BASE_FOLDER.substring(0, BSql.BSQL_BASE_FOLDER.length() - 1);
            new FolderZipper().zipFolder(baseFolder, baseFolder + BACKUP_FILE_POSTFIX);
            logger.info("Archieving complete");
        } catch (IOException ex) {
            boolean yes = SystemInputUtil.captureYesNoInput("Archieving old data failed with unknown cause. "
                    + "It is highly recommended that you archieve your existing data before you proceed with "
                    + "upgrading your data store. Would you like to proceed without creating a backup? (y/n)");
            if (!yes) {
                logger.info("Quitting BlobCity DB due to archive error before version upgrade");
                System.exit(0);
            } else {
                logger.info("Proceeding with data store migration to version " + NEW_VERSION + " without data backup");
            }
        }
    }
    
    public void removeBackup(){
        logger.info("Removing backup archieve");
        final String backupFile = BSql.BSQL_BASE_FOLDER.substring(0, BSql.BSQL_BASE_FOLDER.length() - 1) + BACKUP_FILE_POSTFIX;
        try {
            Files.deleteIfExists(FileSystems.getDefault().getPath(backupFile));
        } catch (IOException ex) {
            logger.error("Failed to remove backup archieve at " + backupFile
                    + ". You may manually delete the file in order to save disk space.");
            java.util.logging.Logger.getLogger(Version1to2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void startUpgrade(){
        upgradeStructure();        
    }
    
    /**
     * NOTE:
     * upgrade database structure
     * create user groups table
     * 
     * NOTE: this can't be done as there is dependency issue if we try to autoWire bean UserGroupManager here
     * SO this is now done, with the help of configBean now
     */
    public void upgradeStructure(){
        
        // add meta to all the tables in all structures. 
        try {
            DirectoryStream<Path> ds = Files.newDirectoryStream(FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER));
            ds.forEach(path -> {
                try {
                    if (!skipAppLevelFolders.contains(path.getFileName().toString()) && Files.isDirectory(path)) {
                        upgradeDataStore(path.getFileName().toString());
                    }
                } catch (Exception ex) {
                    logger.info("Quitting BlobCity DB as upgrade to new version failed. It is recommended that you "
                            + "manually restore the data store from the archieve before attempting a restart.", ex);
                    System.exit(0);
                }
            });
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Version2to3.class.getName()).log(Level.SEVERE, null, ex);
        }    
    }
    
    public void upgradeDataStore(final String datsastoreId){
        logger.info("Upgrading application " + datsastoreId);

        /* Process every table */
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + datsastoreId + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME));
            stream.iterator().forEachRemaining(path -> {
                if (!path.toFile().isDirectory()) {
                    return;
                }

                upgradeCollection(datsastoreId, path.getFileName().toString());
            });
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Version2to3.class.getName()).log(Level.SEVERE, null, ex);
        }
        logger.info("Datastore " + datsastoreId + " upgraded successfully");
    }
    
    /**
     * NOTETOSELF: do this later
     * this will upgrade the table to following structure:
     * flexible schema by default,
     * if primary key is not autoDefined as UUID or timestamp,
     * it will create a new column as UUID defined  id (needs to be discussed)
     * 
     * 
     * NOTE:
     * what it will do now.
     * add meta tag to all the collections and nothing else
     */
    private void upgradeCollection(final String datastoreId, final String collectionId) {
        logger.info("Performing version upgrade of " + datastoreId + "." + collectionId);

        JSONObject oldSchemaJson, newSchemaJson;
        String oldPrimaryKeyColumn;
        SchemaManager schemaManager = new SchemaManager();
        
        /* Update schema to new format */
        try {
            oldSchemaJson = new JSONObject(Files.readAllLines(FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + datastoreId + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME + BSql.SEPERATOR + collectionId + BSql.SCHEMA_FILE)).get(0));
        } catch (IOException ex) {
            logger.error("Upgrade of " + datastoreId + "." + collectionId + " failed. The table may not function correctly.", ex);
            return;
        }
        
        /** upgrading to new schema and adding missing tags **/
        if(oldSchemaJson.has(SchemaProperties.META)){
            return;
        }

            
        JSONObject metaJson = new JSONObject();
        /* Default meta properties of a table when no specific property is specified */
        metaJson.put(SchemaProperties.TABLE_TYPE, TableType.ON_DISK.getType());
        metaJson.put(SchemaProperties.FLEXIBLE_SCHEMA, true);
        metaJson.put(SchemaProperties.REPLICATION_TYPE, ReplicationType.DISTRIBUTED.getType());
        metaJson.put(SchemaProperties.REPLICATION_FACTOR, 0);

        /**  old JSON is the new cols JSON now **/
        /** remove replication factor and replication type from cols json and put it in meta-json **/

        // get old replication values
        metaJson.put(SchemaProperties.REPLICATION_TYPE, oldSchemaJson.getString(SchemaProperties.REPLICATION_TYPE));
        metaJson.put(SchemaProperties.REPLICATION_FACTOR, 0); //force to zero as the convention was changed. Earlier value of 1 meant no replication now value of 0 means no replication.

        // remove replication data from cols json
        oldSchemaJson.remove(SchemaProperties.REPLICATION_TYPE);
        oldSchemaJson.remove(SchemaProperties.REPLICATION_FACTOR);

        // get old current primary key column
        oldPrimaryKeyColumn = oldSchemaJson.getString("primary");

        newSchemaJson = new JSONObject();
        newSchemaJson.put(SchemaProperties.META, metaJson);
        newSchemaJson.put(SchemaProperties.COLS, oldSchemaJson);

        /* Save new schema to schema file */
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(BSql.BSQL_BASE_FOLDER + datastoreId + BSql.SEPERATOR + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME + BSql.SEPERATOR + collectionId + BSql.SCHEMA_FILE))) {
            writer.write(newSchemaJson.toString());
        } catch (IOException ex) {
            logger.error("Upgrade of " + datastoreId + "." + collectionId + " failed. The table may not function correctly.", ex);
            return;
        }

        try {
            schemaManager.insertPrimaryColumn(datastoreId, collectionId);
        } catch (OperationException ex) {
            logger.error("Could not register _id as primary key for " + datastoreId + "." + collectionId);
        }

        /** Migrate all data **/

        /* Create a list of all current primary keys */
        List<String> filenames = new ArrayList<>();
        try (DirectoryStream directoryStream = Files.newDirectoryStream(FileSystems.getDefault().getPath(PathUtil.dataFolderPath(datastoreId, collectionId)))) {
            Iterator<Path> iterator = directoryStream.iterator();

            //TODO: Manually iterator and apply governor limit if required
            iterator.forEachRemaining(path -> {
                String fileName = path.getFileName().toString();
                try {
                    filenames.add(FileNameEncoding.decode(fileName));
                } catch (OperationException ex) {
                    logger.error(null, ex);
                }
            });
        } catch (IOException ex) {
            logger.error(null, ex);
        }

        Schema schema;
        ColumnMapping columnMapping;
        try {
            schema = schemaManager.readSchema(datastoreId, collectionId);
            columnMapping = schemaManager.readColumnMapping(datastoreId, collectionId);
        } catch (OperationException ex) {
            logger.error("Unable to read schema for " + datastoreId + "." + collectionId
                    + ". The collection maybe in corrupt state.");
            return;
        }

        Column primaryColumn = schema.getColumn("_id");

        if(primaryColumn == null) {
            logger.error("No column found with name _id after schema upgrade in " + datastoreId + "." + collectionId
                    + ". Collection maybe corrupted.");
        }

        /* Insert new record with new PK and delete the old record */
        for (String filename : filenames) {

            // load record
            JSONObject internalJson = readRecordFile(datastoreId, collectionId, filename);
            final String newPk = UUID.randomUUID().toString();
            internalJson.put(columnMapping.getInternalName("_id"), newPk);
            writeRecordFile(datastoreId, collectionId, newPk, internalJson);

            // delete old record
            deleteRecordFile(datastoreId, collectionId, filename);
        }

        try {
            schema = schemaManager.readSchema(datastoreId, collectionId);
        } catch (OperationException ex) {
            logger.error("Schema of " + datastoreId + "." + collectionId + " could not be read. Data indexes may be corrupted.");
            return;
        }

        /* Full reindex of all columns */
        BSqlIndexManager indexManager = new BSqlIndexManager();

        Map<String, Column> columnMap = schema.getColumnMap();
        columnMap.forEach((key, column) -> {
            if(!column.getIndexType().equals(IndexTypes.NONE) && !column.getName().equals("_id")) {
                IndexTypes indexType = column.getIndexType();
                try {
                    logger.info("Dropping index for column " + column.getName() + " inside " + datastoreId + "." + collectionId);
                    indexManager.dropIndexOffline(datastoreId, collectionId, column.getName());
                } catch (OperationException ex) {
                    logger.error("Reindex for column " + column.getName() + " in " + datastoreId + "." + collectionId
                            + " failed. index for this column may be corrupted");
                }
            }
        });

        /* Adding commit-logs folder */
        final String absolutePath = PathUtil.tableFolderPath(datastoreId, collectionId);

        /* Make table data directory */
        File file = new File(absolutePath + "/commit-logs");
        if (!file.mkdir()) {
            logger.error("Could not create commit-logs folder inside collection {}.{}", new Object[]{datastoreId, collectionId});
        }

        logger.info("Schema of " + datastoreId + "." + collectionId + " is upgraded. Moving to data upgrade");

        logger.info("Upgrade of " + datastoreId + "." + collectionId + " is successful.");
    }

    private void upgradeData(final String ds, final String collection) {
        logger.info("Upgrading data for " + ds + "." + collection);
    }

    private JSONObject readRecordFile(final String ds, final String collection, final String pk) {
        BSqlFileManager fileManager = new BSqlFileManager();
        JSONObject fileJson;
        try {
            return new JSONObject(fileManager.select(ds, collection, pk));
        } catch (JSONException ex) {
            logger.error("Error in reading old record with primary key " + pk + " during version upgrade of "
                    + ds + "." + collection);
        } catch (OperationException ex) {
            logger.error("Error in reading old record with primary key " + pk + " during version upgrade of "
                    + ds + "." + collection);
        }

        return null;
    }

    private void deleteRecordFile(final String ds, final String collection, final String pk) {
        BSqlFileManager fileManager = new BSqlFileManager();
        try {
            fileManager.remove(ds, collection, pk);
        } catch (OperationException ex) {
            logger.error("Error in deleting old record with primary key " + pk + " during version upgrade of "
                    + ds + "." + collection);
        }
    }

    private void writeRecordFile(final String ds, final String collection, final String pk, final JSONObject internalRecord) {
        BSqlFileManager fileManager = new BSqlFileManager();
        try {
            fileManager.insert(ds, collection, pk, internalRecord.toString());
        } catch (OperationException ex) {
            logger.error("Error in inserting new record with primary key " + pk + " during version upgrade of "
                    + ds + "." + collection);
        }
    }
    
}
