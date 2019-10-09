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

package com.blobcity.db.constants;

/**
 * List of commands used in REST interface
 *
 * @author sanketsarang
 */
public enum BQueryCommands {

    // CRUD
    BULK_SELECT("BULK-SELECT"),
    CONTAINS("CONTAINS"),
    DELETE("DELETE"),
    INSERT("INSERT"),
    SAVE("SAVE"),
    SELECT("SELECT"),
    SELECT_ALL("SELECT-ALL"),
    UPDATE("UPDATE"),

    //TABLE
    CREATE_TABLE("CREATE-TABLE"),
    CREATE_COLLECTION("CREATE-COLLECTION"),
    DROP_COLLECTION("DROP-COLLECTION"),
    DROP_TABLE("DROP-TABLE"),
    VIEW_SCHEMA("VIEW-COLLECTION"),
    LIST_COLLECTIONS("LIST-COLLECTIONS"),
    LIST_TABLES("LIST-TABLES"),
    RENAME_COLLECTION("RENAME-COLLECTION"),
    RENAME_TABLE("RENAME-TABLE"),
    REPOP("REPOP"),
    COLLECTION_EXISTS("COLLECTION-EXISTS"),
    TABLE_EXISTS("TABLE-EXISTS"),
    TRUNCATE_COLLECTION("TRUNCATE-COLLECTION"),
    TRUNCATE_TABLE("TRUNCATE-TABLE"),

    //COLUMN
    ADD_COLUMN("ADD-COLUMN"),
    ALTER_COLUMN("ALTER-COLUMN"),
    CHANGE_DATA_TYPE("CHANGE-DATA-TYPE"),
    DROP_COLUMN("DROP-COLUMN"),
    DROP_INDEX("DROP-INDEX"),
    DROP_UNIQUE("DROP-UNIQUE"),
    INDEX("INDEX"),
    RENAME_COLUMN("RENAME-COLUMN"),
    SET_AUTO_DEFINE("SET-AUTO-DEFINE"),

    //SEARCH
    SEARCH("SEARCH"),
    SEARCH_AND("SEARCH-AND"),
    SEARCH_AND_LOAD("SEARCH-AND-LOAD"),
    SEARCH_OR("SEARCH-OR"),
    SEARCH_OR_LOAD("SEARCH-OR-LOAD"),
    SEARCH_FILTERED("SEARCH-FILTERED"),
    INSERT_CUSTOM("INSERT-CUSTOM"),

    //DATASTORE
    CREATE_DB("CREATE-DB"),
    CREATE_DS("CREATE-DS"),
    DROP_DB("DROP-DB"),
    DROP_DS("DROP-DS"),
    LIST_DS("LIST-DS"),
    TRUNCATE_DS("TRUNCATE-DS"),
    DS_EXISTS("DS-EXISTS"),

    BULK_EXPORT("BULK-EXPORT"),
    BULK_IMPORT("BULK-IMPORT"),
    LIST_OPS("LIST-OPS"),
    RESET_USAGE("RESET-USAGE"),
    SET_LIMITS("SET-LIMITS"),
    USAGE("USAGE"),

    //FTP Service
    START_DS_FTP("START-DS-FTP"),
    STOP_DS_FTP("STOP-DS-FTP"),


    //TABLEAU
    TABLEAU_REQUIRES_SYNC("TABLEAU-REQUIRES-SYNC"),


    //USERNAME-CODE
    CHANGE_TRIGGER("CHANGE-TRIGGER"),
    LIST_FILTERS("LIST-FILTERS"),
    LIST_TRIGGERS("LIST-TRIGGERS"),
    LOAD_CODE("LOAD-CODE"),
    REGISTER_PROCEDURE("REGISTER-PROCEDURE"),
    REGISTER_TRIGGER("REGISTER-TRIGGER"),
    UNREGISTER_PROCEDURE("UNREGISTER-PROCEDURE"),
    UNREGISTER_TRIGGER("UNREGISTER-TRIGGER"),
    SP("SP"),

    //USERNAME
    ADD_USER("ADD-USERNAME"),
    CHANGE_PASSWORD("CHANGE-PASSWORD"),
    DROP_USER("DROP-USERNAME"),
    VERIFY_CREDENTIALS("VERIFY-CREDENTIALS"),
    CREATE_MASTER_KEY("CREATE-MASTER-KEY"),
    CREATE_DS_KEY("CREATE-DS-KEY"),
    LIST_API_KEYS("LIST-API-KEYS"),
    LIST_DS_API_KEYS("LIST-DS-API-KEYS"),
    DROP_API_KEY("DROP-API-KEY"),

    //USERNAME GROUPS
    CREATE_GROUP("CREATE-GROUP"),
    DROP_GROUP("DROP-GROUP"),
    ADD_TO_GROUP("ADD-TO-GROUP"),
    REMOVE_FROM_GROUP("REMOVE-FROM-GROUP"),

    // CLUSTER
    NODE_ID("NODE-ID"),
    ADD_NODE("ADD-NODE"),
    LIST_NODES("LIST-NODES");
    
    final String command;

    BQueryCommands(final String command) {
        this.command = command;
    }

    public final String getCommand() {
        return command;
    }

    public static BQueryCommands fromString(final String commandString) {
        for (BQueryCommands bQueryCommand : BQueryCommands.values()) {
            if (bQueryCommand.getCommand().equalsIgnoreCase(commandString)) {
                return bQueryCommand;
            }
        }

        return null;
    }
}
