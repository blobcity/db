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

package com.blobcity.db.exceptions;

/**
 * Enum for various errorcodes used in the database
 *
 * @author sanketsarang
 */
public enum ErrorCode {

    ADD_COLUMN_ERROR("ADD_COLUMN_ERROR", "Add column operation failed"), 
    ADD_NODE_ERROR("ADD_NODE_ERROR", "Error executing the add-node operation for adding a node into the cluster"), 
    ALREADY_INDEXED("ALREADY_INDEXED", "Item already indexed"), 
    ALREADY_LOCKED("DBIEX", "Internal error: Requested item is locked"),
    APP_INVALID("APP003", "Invalid application"),
    CANNOT_REGISTER_OPERATION("CANNOT_REGISTER_OPERATION", "New operation registration failed"),
    CLASS_LOAD_ERROR("DB400", "Error loading class"),
    CLUSTER_ADD_NODE_VALIDATION_FAILED("CLUSTER_ADD_NODE_VALIDATION_FAILED", "Cluster add node validation failed"),
    CLUSTER_CONNECTION_ERROR("CLUSTER_CONNECTION_ERROR", "Problem with cluster socket connection"),
    CLUSTER_MESSAGE_FORMAT_ERROR("CLUSTER_MESSAGE_FORMAT_ERROR", "Message format for internal cluster communication message incorrect"),
    CODE_MANIFEST_PARSING_ERROR("DB403", "Code manifest file could not be successfully parsed"),
    COLUMN_INVALID("COLUMN_INVALID", "Column invalid"),
    CONFIG_FILE_ERROR("CONFIG_FILE_ERROR", "Error occured in reading / writing / processing the configuration file"),
    DATASTORE_ALREADY_EXISTS("DATASTORE_ALREADY_EXISTS", "A datastore with the given name is already present"),
    DATASTORE_DELETION_ERROR("DBIEX", "Internal Error: Datastore deletion failed"),
    DATASTORE_INVALID("DATASTORE_INVALID", "Invalid datastore"),
    DATAINTERPRETER_EXECUTION_ERROR("DATAINTERPRETER_EXECUTION_ERROR", "Error while executing data interpreter"),
    DATAINTERPRETER_INCORRECT_PARAMS("DATAINTERPRETER_INCORRECT_PARAMS", "Incorrect params provided"),
    DATAINTERPRETER_LOAD_ERROR("DATAINTERPRETER_LOAD_ERROR","DataInterpreter Load Error"),
    DATAINTERPRETER_NOT_LOADED("DATAINTERPRETER_NOT_LOADED", "Specified data interpreter is not loaded"),
    DATATYPE_CONSTRAINT_VIOLATION("DATATYPE_CONSTRAINT_VIOLATION", "Violation of constraint set on the data type"),
    DATATYPE_ERROR("DATATYPE_ERROR", "An unknown internal error occured in processing data types"), 
    DATATYPE_MISMATCH("DATATYPE_MISMATCH", "Datatypes do not match"), 
    DROP_COLUMN_ERROR("DROP_COLUMN_ERROR", "Drop column operation failed"), 
    DUPLICATE_COLUMN_NAME("DUPLICATE_COLUMN_NAME", "Column with duplicate name not permitted"),
    DUPLICATE_OPERATION("DUPLICATE_OPERATION", "Duplication operation encountered"),
    DUPLICATE_COLLECTION_NAME("DUPLICATE_COLLECTION_NAME", "Duplication collection name"),
    FEATURE_RESTRICTED("FEATURE_RESTRICTED", "This feature is not available as part of your current license"),
    FILTER_EXECUTION_ERROR("FILTER_EXECUTION_ERROR", "Error while executing filter"),
    FILTER_INCORRECT_PARAMS("FILTER_INCORRECT_PARAMS", "Incorrect params provided"),
    FILTER_LOAD_ERROR("FILTER_LOAD_ERROR", "Filter Load Error"),
    FILTER_NOT_LOADED("FILTER_NOT_LOADED", "Specified filter is not loaded"),
    IMPORT_FILE_NOT_AVAILABLE("IMPORT_FILE_NOT_AVAILABLE", "Import file not available"),
    IMPORT_FILE_HEADER_MISSING("IMPORT_FILE_HEADER_MISSING", "Header row missing in import file"),
    INADEQUATE_FILE_SYSTEM_PERMISSION("DBIEX", "Internal error: Inadequate file system permission"),
    INDEXING_ERROR("INDEXING_ERROR", "Error occured during index operation"), 
    INDEX_COUNT_ERROR("DBIEX", "Internal index size counting logic failure"), 
    INDEX_UNIQUE_CONFLICT("INDEX_UNIQUE_CONFLICT", "Constraint violation. Prospective data for unique indexing conincides with an already indexed value"), 
    INEXISTENT_OPERATION("INEXISTENT_OPERATION", "Requested operation is inexistent"),
    INSERT_ERROR("INSERT_ERROR", "Insert operation failed"),
    INTERNAL_OPERATION_ERROR("DBIEX", "An unknown error occured internal to the datastore .Please contact your system administrators for resolving this error."),
    INVALID_DATASTORE_NAME("INVALID_DATASTORE_NAME", "Not a valid datastore name"),
    INVALID_DATE_FORMAT("INVALID_DATE_FORMAT", "Date format invalid. Supported format is yyyy-[m]m-[d]d"),
    INVALID_FIELD_LENGTH_FORMAT("INVALID_FIELD_LENGTH_FORMAT", "Supported format for field length specification is: length[{K|M|G}]"),
    INVALID_NODE_ID("INVALID_NODE_ID", "Node id invalid"),
    INVALID_NUMBER_FORMAT("INVALID_NUMBER_FORMAT", "Number format is invalid"),
    INVALID_OPERATION_FORMAT("DBIEX", "Internal error: Invalid operation format"),
    INVALID_OPERATOR_USAGE("INVALID_OPERATOR_USAGE", "Invalid operator usage"),
    INVALID_QUERY("DBQEX", "Specified query could not be identified as any valid query"),
    INVALID_QUERY_FORMAT("DBQEX", "Query format invalid"),
    INVALID_QUERY_PARAMETER("INVALID_QUERY_PARAMETER", "Query parameter invalid"),
    INVALID_REF_FORMAT("INVALID_REF_FORMAT", "Supported format REF is [table UUID]:[row UUID]"),
    INVALID_SCHEMA("INVALID_SCHEMA", "Invalid schema"),
    INVALID_SEARCH_CRITERIA("INVALID_SEARCH_CRITERIA", "Search criteria is invalid"),
    INVALID_TIMESTAMP_FORMAT("INVALID_TIMESTAMP_FORMAT", "Timestamp format invalid. Supported format is yyyy-[m]m-[d]d hh:mm:ss[.f...]"),
    INVALID_TIME_FORMAT("INVALID_TIME_FORMAT", "Time format invalid. Supported format is hh:mm:ss"),
    LICENSE_EXPIRED("LICENSE_EXPIRED", "License expired"),
    LOCKING_ERROR("DBIEX", "Internal error: Locking operation failed"),
    NOT_INDEXED("NOT_INDEXED", "Item not indexed"),
    NOT_LOCKED("DBIEX", "Internal error: Item not locked"), 
    OPERATION_FILE_ERROR("DBIEX", "Internal error: Operation descriptor file is possibly corrupted"),
    OPERATION_LOGGING_ERROR("DBIEX", "Internal error: Operation level logging failed"), 
    OPERATION_NOT_LOADED("DBIEX", "Internal error: Operation is not loaded"), 
    OPERATION_NOT_SUPPORTED("DBIEX", "The requested operation is not supported."),
    PRIMARY_KEY_CONFLICT("DB201", "Constraint violation. Intended operation will result in duplicate entry for primary key"),
    PRIMARY_KEY_INDEX_DROP_RESTRICTED("PRIMARY_KEY_INDEX_DROP_RESTRICTED", "Primary key index cannot be dropped"),
    PRIMARY_KEY_INEXISTENT("DB200", "No record found matching the requested primary key"),
    RENAME_COLLECTION_ERROR("RENAME_COLLECTION_ERROR", "Collection rename operation failed"),
    SCHEMA_CHANGE_NOT_PERMITTED("SCHEMA_CHANGE_NOT_PERMITTED", "Schema change not permitted"),
    SCHEMA_CORRUPTED("DBIEX", "Internal error: Schema corrupted"),
    SCHEMA_FILE_NOT_FOUND("DBIEX", "Internal error: Schema file not found"),
    SCHEMA_FILE_READ_FAILED("DBIEX", "Internal error: Schema file read failed"),
    SCHEMA_NOT_CACHED("DBIEX", "Internal error: Schema not cached"),
    SELECT_ERROR("SELECT_ERROR", "Select operation failed"),
    STORED_PROCEDURE_EXECUTION_ERROR("DB406", "Error executing stored procedure"), 
    STORED_PROCEDURE_INCORRECT_PARAMS("DB405", "Incorrect parameters passed to requested stored procedure"),
    STORED_PROCEDURE_LOAD_ERROR("DB401", "Error loading stored procedure"),
    STORED_PROCEDURE_NOT_LOADED("DB404", "Requested stored procedure is not loaded"),
    STRING_LENGTH_EXCEEDED("STRING_LENGTH_EXCEEDED", "Length of the string exceeded the maximum permissible limit"),
    COLLECTION_CREATION_ERROR("DBIEX", "Intenral error: Collection creation failed"),
    COLLECTION_DELETION_ERROR("DBIEX", "Intrernal error: Collection deletion failed"),
    COLLECTION_INVALID("DB001", "Invalid collection"),
    COLLECTION_ROW_COUNT_ERROR("COLLECTION_ROW_COUNT_ERROR", "Error in maintaining/processing collection row count"),
    COLLECTION_TYPE_ERROR("COLLECTION_TYPE_ERROR", "Error with collection type specification"),
    TRIGGER_EXECUTION_ERROR("TRIGGER_EXECUTION_ERROR", "Error executing trigger"),
    TRIGGER_LOAD_ERROR("DB402", "Error loading trigger"),
    TRIGGER_NOT_LOADED("DB410", "Requested trigger is not loaded"),
    UNIQUE_INDEX_READ_AS_STREAM_NOT_PERMITTED("UNIQUE_INDEX_READ_AS_STREAM_NOT_PERMITTED", "Reading as stream for an unique type index is not permitted"),
    UNKNOWN_COLUMN("UNKNOWN_COLUMN", "Unknown column"),
    UNKNOWN_NODE_ID("UNKNOWN_NODE_ID", "Node id unknown"),
    UPDATE_OPERATION_ERROR("UPDATE_OPERATION_ERROR","Update operation failed"),
    USER_CREDENTIALS_INVALID("APP003", "Invalid credentials"),
    INVALID_API_KEY("INVALID_API_KEY", "API key invalid"),
    DATA_FILE_NOT_FOUND("DATA_FILE_NOT_FOUND","Data file not found. Check the file path"),
    UNKNOWN_ERROR("UNKNOWN_ERROR","An unknown error occurred"),
    ACF_PARSE_ERROR("ACF_PARSE_ERROR", "Error occured in parsing ACF"),
    DEPRICATED("DEPRICATED", "The requested operation is depricated and no longer support in this version"),
    INVALID_REQUEST_ID("INVALID_REQUEST_ID", "No request currently active with the specified id"),
    NOT_AUTHORISED("NOT AUTHORISED", "Not authorised to perform the operation"),
    UNSUPPORTED_FORMAT_CONVERSION("UNSUPPORTED_FORMAT_CONVERSION","Unsupported format conversion"),
    UNRECOGNISED_DOCUMENT_FORMAT("UNRECOGNISED_DOCUMENT_FORMAT", "Unrecognised document format"),
    INVALID_WEBSERVICE_ENDPOINT("INVALID_WEBSERVICE_ENDPOINT", "No customer web-service registered at the specified endpoint"),
    CODE_LOAD_ERROR("CODE_LOAD_ERROR", "Unable to load code"),
    TABLEAU_EXCEPTION("TABLEAU_EXCEPTION", "Internal error with Tableau integration"),
    GROUP_BY("GROUP_BY", "Error executing GROUP BY clause"),
    EXCEL_DATA_READ_ERR("EXCEL_DATA_READ_ERR", "Error reading data in Excel format");
    
    private final String errorCode;
    private final String errorMessage;

    ErrorCode(final String errorCode) {
        this.errorCode = errorCode;
        this.errorMessage = "";
    }

    ErrorCode(final String errorCode, final String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getErrorCode() {
        return this.errorCode;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public static ErrorCode fromString(final String codeString) {
        for(ErrorCode errorCode : ErrorCode.values()) {
            if(errorCode.getErrorCode().equalsIgnoreCase(codeString)) {
                return errorCode;
            }
        }

        return null;
    }
}
