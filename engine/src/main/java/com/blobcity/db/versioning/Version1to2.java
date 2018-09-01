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

import com.blobcity.db.constants.BSql;
import com.blobcity.db.table.TableStructure;
import com.blobcity.db.table.indexes.Column;
import com.blobcity.db.util.SystemInputUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class Version1to2 implements VersionUpgrader {

    private static final String OLD_VERSION = "1.0";
    private static final String NEW_VERSION = "1.1";
    private static final String BACKUP_FILE_POSTFIX = "-v1.zip";
    private static final Logger logger = LoggerFactory.getLogger(Version1to2.class.getName());
    int zipCount = 0;
    private static final boolean DEBUG = true;

    @Override
    public void upgrade() {
        try {

            /* Check if data is in version 1 or version 2. This is because version 1&2 data store were not self version aware */
            if (!isVersion1Data()) {
                return;
            }

            logger.info("Upgrading version from " + OLD_VERSION + " to " + NEW_VERSION + " . . .");

            /* Create backup */
            createBackup();

            /* Perform actual data upgrade */
            upgradeData();

            /* Remove backup copy */
            removeBackup();

            /* Remove any traces of old copy of the data. This applies to version 1 only */
            cleanTraces();
        } catch (Exception ex) {
            logger.error("Data storage format upgrade to version " + NEW_VERSION + " failed. The application may not function correctly.");
            java.util.logging.Logger.getLogger(Version1to2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void log(String log) {
        if (DEBUG) {
            logger.debug(log);
        }
    }

    private void createBackup() {
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

    private void upgradeData() {
        logger.info("Migrating data to new storage format");
        long startTime = System.currentTimeMillis();
        try {
            DirectoryStream<Path> ds = Files.newDirectoryStream(FileSystems.getDefault().getPath(BSql.OLD_BSQL_BASE_FOLDER));
            Iterator<Path> iterator = ds.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (Files.isDirectory(path)) {
                    portApplication(path.getFileName().toString());
                }
            }
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Version1to2.class.getName()).log(Level.SEVERE, null, ex);
        }
        long elapsedTime = System.currentTimeMillis() - startTime;
        logger.info("Data migration to new storage format successfully completed in " + elapsedTime + "ms");
    }

    private void removeBackup() {
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

    private void cleanTraces() {
        Path path = null;
        try {
            path = FileSystems.getDefault().getPath(BSql.OLD_BSQL_BASE_FOLDER);
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

            });
        } catch (IOException ex) {
            logger.error("Failed to delete old copy of data. You may manually delete the folder located at: " + path.toAbsolutePath().toString());
            java.util.logging.Logger.getLogger(Version1to2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void portApplication(final String appId) {
        logger.info("Migrating data of application: " + appId);
        final String sourceAppFolder = BSql.OLD_BSQL_BASE_FOLDER + appId;
        String newAppId = null;
        Path destinationAppPath = FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + appId);

        /* Check if destination folder isPresent */
        while (Files.exists(destinationAppPath)) {
            if (newAppId == null) {
                if (SystemInputUtil.captureYesNoInput("An application with the name " + appId + " already isPresent in new "
                        + "storage format. Would you like to skip data migration of this application? (y/n)")) {
                    return;
                }
            } else {
                if (SystemInputUtil.captureYesNoInput("The new application name " + newAppId
                        + " entered for application " + appId + " conincides with another application. "
                        + "Would you wnat to skip migration of this application? (y/n)")) {
                    return;
                }
            }

            newAppId = SystemInputUtil.captureLineInput("Enter a new name for application " + appId);
            destinationAppPath = FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + newAppId);
        }

        if (newAppId == null) {
            newAppId = appId; //this means the application id is retained same during migration. This is default case.
        }

        /* Create application folders */
        createAppFolders(newAppId);

        File file = new File(sourceAppFolder);
        File[] databases = file.listFiles();
        for (File database : databases) {
            if (database.isFile() || "db".equals(database.getName())) {
                continue;
            }

            File[] tables = database.listFiles();

            for (File table : tables) {
                if (table.isFile()) {
                    continue;
                }

                portTable(appId, newAppId, table.getName(), database.getName());
            }
        }
        logger.info("Completed data migration for application: " + appId);
    }

    private void createAppFolders(final String appId) {
        logger.debug("Creating application folders for " + appId);
        final String appLocation = BSql.BSQL_BASE_FOLDER + appId;
        new File(appLocation).mkdir();
        new File(appLocation + "/db/").mkdir();
        new File(appLocation + "/code/").mkdir();
        new File(appLocation + "/code/app/").mkdir();
        new File(appLocation + "/code/db/").mkdir();
        new File(appLocation + "/del/").mkdir();
        new File(appLocation + "/deploy-db-hot/").mkdir();
        new File(appLocation + "/deployed-versions/").mkdir();
        new File(appLocation + "/export/").mkdir();
        new File(appLocation + "/logs/").mkdir();
        new File(appLocation + "/uploads/").mkdir();
        logger.debug("Created application folders for " + appId);
    }

    private void portTable(final String oldAppId, final String newAppId, final String oldTableName, final String database) {
        logger.info("Porting table " + oldAppId + " -> " + database + " -> " + oldTableName);

        final String newTableName = getNewTableName(newAppId, oldTableName);

        /* Create table folders */
        createTableFolders(newAppId, newTableName);

        /* Migrate schema file */
        logger.debug("Creating schema file for " + newAppId + " -> " + oldTableName);
        String sourceFile = BSql.OLD_BSQL_BASE_FOLDER + oldAppId + "/" + database + "/" + oldTableName + "/schema.bsc";
        String destinationFile = BSql.BSQL_BASE_FOLDER + newAppId + "/db/" + newTableName + "/meta/schema.bdb";
        JSONObject schemaJson = createSchemaFile(sourceFile, destinationFile);
        if (schemaJson == null) {
            logger.error("Skipping porting of data inside " + oldAppId + " -> " + oldTableName + " due to an error");
            return;
        } else {
            logger.debug("Successfully created schema file for " + newAppId + " -> " + newTableName);
        }

        /* Create column-mapping file */
        logger.debug("Registering internal column mappings for table " + newAppId + " -> " + newTableName);
        destinationFile = BSql.BSQL_BASE_FOLDER + newAppId + "/db/" + newTableName + "/meta/column-mapping.bdb";
        Map<String, String> columnMap = createColumnMappingFile(destinationFile, schemaJson);
        if (columnMap == null) {
            logger.error("Skipping porting of data inside " + oldAppId + " -> " + oldTableName + " due to an error");
            return;
        } else {
            logger.debug("Successfully created internal column mappings for table " + newAppId + " -> " + newTableName);
        }

        /* Port data */
        if (oldAppId.equals(newAppId) && oldTableName.equals(newTableName)) {
            logger.info("Porting data points inside " + oldAppId + " -> " + oldTableName);
        } else {
            logger.info("Porting data points inside " + oldAppId + " -> " + oldTableName + " to " + newAppId + " -> " + newTableName);
        }
        sourceFile = BSql.OLD_BSQL_BASE_FOLDER + oldAppId + "/" + database + "/" + oldTableName + "/data/";
        destinationFile = BSql.BSQL_BASE_FOLDER + newAppId + "/db/" + newTableName + "/data/";
        portData(sourceFile, destinationFile, columnMap);
        if (oldAppId.equals(newAppId) && oldTableName.equals(newTableName)) {
            logger.info("Successfully ported table " + oldAppId + " -> " + oldTableName);
        } else {
            logger.info("Successfully ported table " + oldAppId + " -> " + oldTableName + " to " + newAppId + " -> " + newTableName);
        }
    }

    private void createTableFolders(final String appId, final String table) {
        logger.debug("Creating table folders for " + appId + " -> " + table);
        final String tableLocation = BSql.BSQL_BASE_FOLDER + appId + "/db/" + table;
        new File(tableLocation).mkdir();
        new File(tableLocation + "/data/").mkdir();
        new File(tableLocation + "/export/").mkdir();
        new File(tableLocation + "/import/").mkdir();
        new File(tableLocation + "/index/").mkdir();
        new File(tableLocation + "/meta/").mkdir();
        new File(tableLocation + "/ops/").mkdir();
        logger.debug("Created table folders for " + appId + " -> " + table);
    }

    private void portData(final String sourceDataFolder, final String destinationDataFolder, final Map<String, String> columnMap) {
        Iterator<Path> dataIterator;
        try {
            dataIterator = Files.newDirectoryStream(FileSystems.getDefault().getPath(sourceDataFolder)).iterator();
        } catch (IOException ex) {
            logger.error("Failed to read data at " + sourceDataFolder, ex);
            return;
        }

        dataIterator.forEachRemaining(path -> {
            try {
                String contentsString = new String(Files.readAllBytes(path));
                JSONObject newDataJson = getMappedColumnData(new JSONObject(contentsString), columnMap);
                String fileName = path.getFileName().toString();
                fileName = fileName.substring(0, fileName.length() - 4); //this is for dropping .bdb extensions which were added in version 1.
                Path destinationPath = FileSystems.getDefault().getPath(destinationDataFolder + fileName);
                Files.write(destinationPath, newDataJson.toString().getBytes());
            } catch (IOException ex) {
                logger.error("Failed to migrate data point located at: " + path.toAbsolutePath().toString());
                java.util.logging.Logger.getLogger(Version1to2.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
    }

    private Map<String, String> createColumnMappingFile(final String columnMappingFile, final JSONObject schemaJson) {
        Map<String, String> map = new HashMap<>();
        Iterator<String> keysIterator = schemaJson.keys();
        int index = 0;
        while (keysIterator.hasNext()) {
            String key = keysIterator.next();
            if ("primary".equals(key)) {
                continue;
            }
            map.put(key, "" + ++index);
        }

        Map<String, Object> mappingFileMap = new HashMap<>();
        mappingFileMap.put("index", index);
        mappingFileMap.put("map", map);
        JSONObject columnMappingJson = new JSONObject(mappingFileMap);

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(columnMappingFile)))) {
            writer.write(columnMappingJson.toString());
        } catch (FileNotFoundException ex) {
            logger.error("Failed to create column-mapping file " + columnMappingFile, ex);
            return null;
        } catch (IOException ex) {
            logger.error("Failed to create column-mapping file " + columnMappingFile, ex);
            return null;
        }

        return map;
    }

    private JSONObject createSchemaFile(final String sourceSchemaFile, final String destinationSchemaFile) {

        JSONObject schemaJson;
        TableStructure tableStructure;
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> schemaMap = new HashMap<>();

        File schemaFile = new File(sourceSchemaFile);
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(schemaFile))) {
            tableStructure = (TableStructure) ois.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            logger.error("Unable to parse schema file " + sourceSchemaFile + ". The respective table may not function correctly.", ex);
            return null;
        }

        String primaryKeyName = tableStructure.getPrimaryKey().get(0).toString();
        schemaMap.put("primary", primaryKeyName);

        //TODO: Read table structure from table folder
        Column column = tableStructure.getColumns();
        for (Object o : column.keySet()) {
            Map<String, Object> columnMap = new HashMap<>();

            /* Data type */
            String type = column.get(o).toString();
            if ("integer".equals(type)) {
                type = "int";
            } else if ("bit".equals(type)) {
                type = "char";
            }
            columnMap.put("type", type);

            /* Index */
            if (primaryKeyName.equals(o.toString())) {
                columnMap.put("index", "UNIQUE");
            } else {
                columnMap.put("index", "NONE");
            }

            /*Auto define */
            if (tableStructure.getAutoNumbered().containsKey(o)) {
                columnMap.put("auto-define", "UUID");
            } else {
                columnMap.put("auto-define", "NONE");
            }

            schemaMap.put(o.toString(), columnMap);
        }

        schemaJson = new JSONObject(schemaMap);

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destinationSchemaFile)))) {
            writer.write(schemaJson.toString());
        } catch (FileNotFoundException ex) {
            logger.error("Failed to create schema file " + destinationSchemaFile, ex);
            return null;
        } catch (IOException ex) {
            logger.error("Failed to create schema file " + destinationSchemaFile, ex);
            return null;
        }

        return schemaJson;
    }

    private JSONObject getMappedColumnData(final JSONObject nonMappedJson, final Map<String, String> columnMap) {
        final JSONObject mappedJson = new JSONObject();
        Iterator<String> keys = nonMappedJson.keys();
        keys.forEachRemaining(key -> {
            if (columnMap.containsKey(key)) {
                mappedJson.put("" + columnMap.get(key), nonMappedJson.get(key));
            }
        });

        return mappedJson;
    }

    private boolean isVersion1Data() {
        return Files.exists(FileSystems.getDefault().getPath(BSql.OLD_BSQL_BASE_FOLDER));
    }

    private String getNewTableName(final String newAppId, final String oldTableName) {
        int index = 1;
        String newTableName = oldTableName;
        Path path = FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + newAppId + "/db/" + newTableName);
        while (Files.exists(path)) {
            path = FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + newAppId + "/db/" + newTableName + (index++));
        }

        return newTableName;
    }
}
