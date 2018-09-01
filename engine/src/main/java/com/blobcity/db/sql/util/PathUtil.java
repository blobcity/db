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

package com.blobcity.db.sql.util;

import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.util.FileNameEncoding;

/**
 *
 * @author sanketsarang
 */
public class PathUtil {

    public static String tableFolderPath(final String appId, final String table) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        return path.toString();
    }

    public static String dataFolderPath(final String appId, final String table) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.DATA_FOLDER);
        return path.toString();
    }

    public static String schemaFilePath(final String appId, final String table) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.SCHEMA_FILE);
        return path.toString();
    }

    /**
     * Gets the absolute location of the table row count file on the file system. The returned path is to the file
     * located at BC_HOME/{app}/db/{table}/meta/row-count.bdb
     *
     * @param appId the application id of the BlobCity application
     * @param table name of table within the application
     * @return absolute location of the row count file on the file system for the specified table within the specified
     * application
     */
    public static String tableRowCountFilePath(final String appId, final String table) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.TABLE_ROW_COUNT_FILE);
        return path.toString();
    }

    public static String columnMappingFilePath(final String appId, final String table) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.COLUMN_MAPPING_FILE);
        return path.toString();
    }

    public static String dataFile(final String appId, final String table, final String pk) throws OperationException {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.DATA_FOLDER);
        path.append(FileNameEncoding.encode(pk));
        return path.toString();
    }

    public static String indexFolder(final String appId, final String table) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.INDEX_FOLDER);
        return path.toString();
    }

    public static String indexColumnFolder(final String appId, final String table, final String column) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.INDEX_FOLDER);
        path.append(column);
        path.append(BSql.SEPERATOR);
        return path.toString();
    }

    public static String indexColumnValueFolder(final String appId, final String table, final String column, final String value) throws OperationException {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.INDEX_FOLDER);
        path.append(column);
        path.append(BSql.SEPERATOR);
        path.append(FileNameEncoding.encode(value));
        path.append(BSql.SEPERATOR);
        return path.toString();
    }

    public static String globalDeleteFolder(final String name) {
        StringBuilder path = new StringBuilder(BSql.GLOBAL_DELETE_FOLDER);
        path.append(name);
        return path.toString();
    }

    public static String operationFolder(final String appId, final String table) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.OPERATION_FOLDER);
        return path.toString();
    }

    public static String operationFile(final String appId, final String table, final String opid) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.OPERATION_FOLDER);
        path.append(opid);
        return path.toString();
    }

    public static String operationLogFile(final String appId, final String table, final String opid) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.OPERATION_FOLDER);
        path.append(opid);
        path.append(".log");
        return path.toString();
    }

    public static String importFile(final String appId, final String table, final String fileName) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.IMPORT_FOLDER);
        path.append(fileName);
        return path.toString();
    }

    /**
     * Gets path to the file BC_HOME/{app}/db/{table}/index-count/{column}/{columnValue}
     *
     * @param appId the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue the stored index value. Must be the hashed value in case of the columns indexing type being
     * hashed
     * @return absolute file system path to the index count file for the specified column and index value
     */
    public static String indexCountFile(final String appId, final String table, final String column, final String columnValue) throws OperationException {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.INDEX_COUNT_FOLDER);
        path.append(column);
        path.append(BSql.SEPERATOR);
        path.append(FileNameEncoding.encode(columnValue));
        return path.toString();
    }

    /**
     * Gets path to the folder BC_HOME/{app}/db/{table}/index-count/{column}
     *
     * @param appId the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @return absolute file system path to the index count store for the specified column
     */
    public static String indexCountColumnFolder(final String appId, final String table, final String column) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(table);
        path.append(BSql.INDEX_COUNT_FOLDER);
        path.append(column);
        return path.toString();
    }

    public static String databaseFolder(final String appId) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        return path.toString();
    }

    public static String globalLiveFile(final String fileName) {
        StringBuilder path = new StringBuilder(BSql.GLOBAL_LIVE_FOLDER);
        path.append(fileName);
        return path.toString();
    }

    public static String exportFile(final String appId, final String filePath) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(appId);
        path.append(BSql.SEPERATOR);
        path.append(BSql.FTP_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(filePath);
        return path.toString();
    }

    public static String codeManifestFile(final String app) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(app);
        path.append(BSql.DB_HOT_DEPLOY_FOLDER);
        path.append(BSql.MANIFEST_FILE_NAME);
        return path.toString();
    }

    public static String getCustomCodeJarFilePath(final String ds, final String jarFilePath) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(ds);
        path.append(BSql.SEPERATOR);
        path.append(BSql.FTP_FOLDER_NAME);
        path.append(jarFilePath);
        return path.toString();
    }

    public static String ftpUserCredentialsFolder() {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(BSql.FTP_USER_CREDENTIALS_FOLDER);
        return path.toString();
    }

    public static String ftpUserCredentialsFile() {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(BSql.FTP_USER_CREDENTIALS_FOLDER);
        path.append(BSql.SEPERATOR);
        path.append(BSql.FTP_USER_CREDENTIALS_FILE);
        return path.toString();
    }

    public static String datastoreFtpFolder(final String ds) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(ds);
        path.append(BSql.SEPERATOR);
        path.append(BSql.FTP_FOLDER_NAME);
        return path.toString();
    }

    public static String tableauConfigFile() {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(BSql.RESOURCES);
        path.append(BSql.SEPERATOR);
        path.append(BSql.TABLEAU_CONFIG_FILENAME);
        return path.toString();
    }

    public static String tableauSchemaFile() {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(BSql.RESOURCES);
        path.append(BSql.SEPERATOR);
        path.append(BSql.TABLEAU_SCHEMA_FILENAME);
        return path.toString();
    }

    public static String tableauTdeFile(final String ds, final String collection) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(ds);
        path.append(BSql.SEPERATOR);
        path.append(BSql.DATABASE_FOLDER_NAME);
        path.append(BSql.SEPERATOR);
        path.append(collection);
        path.append(BSql.EXPORT_FOLDER);
        path.append(BSql.TABLEAU_TDE_FILENAME);
        return path.toString();
    }
}
