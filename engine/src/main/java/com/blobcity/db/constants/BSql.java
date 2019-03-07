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

import java.io.File;

/**
 * Saves constants relating the BSql file storage.
 *
 * @author sanketsarang
 */
public final class BSql {

    public static final String SEPERATOR = File.separator;
    public static final String FOLDER_UP = ".." + SEPERATOR;
    public static final String BSQL_BASE_FOLDER = System.getenv("BLOBCITY_DATA") == null ?
            System.getProperty("user.dir") + SEPERATOR + ".." + SEPERATOR + "data" + SEPERATOR
            : System.getenv("BLOBCITY_DATA");
    
    public static final String OLD_BSQL_BASE_FOLDER = BSQL_BASE_FOLDER + SEPERATOR + "BlobCityDB" + SEPERATOR; // WARN: changing this may affect version porting scripts
    public static final String DATABASE_FOLDER_NAME = "db";
    public static final String DELETE_FOLDER = SEPERATOR + "del" + SEPERATOR;
    public static final String DATA_FOLDER = SEPERATOR + "data" + SEPERATOR;
    public static final String COMMIT_LOGS_FOLDER = BSQL_BASE_FOLDER + "commit-logs" + SEPERATOR;
    public static final String COMMIT_LOGS_FOLDER_NAME = "commit-logs" + SEPERATOR;
    public static final String FTP_FOLDER_NAME = "ftp" + SEPERATOR;
    public static final String CURRENT_COMMIT_LOG_FILENAME = "commit.log";
    public static final String CURRENT_COMMIT_LOG_FILE = COMMIT_LOGS_FOLDER + "commit.log";
    public static final String INDEX_FOLDER = SEPERATOR + "index" + SEPERATOR;
    public static final String META_FOLDER = SEPERATOR + "meta" + SEPERATOR;
    public static final String OPERATION_FOLDER = SEPERATOR + "ops" + SEPERATOR;
    public static final String DB_HOT_DEPLOY_FOLDER = SEPERATOR + "deploy-db-hot" + SEPERATOR;
    public static final String IMPORT_FOLDER = SEPERATOR + "import" + SEPERATOR;//app/db/table/import
    public static final String EXPORT_FOLDER = SEPERATOR + "export" + SEPERATOR;//app/export
    public static final String SCHEMA_FILE = SEPERATOR + "meta" + SEPERATOR + "schema.bdb";
    public static final String TABLE_ROW_COUNT_FILE = SEPERATOR + "meta" + SEPERATOR + "row-count.bdb";
    public static final String INDEX_COUNT_FOLDER = SEPERATOR + "index-count" + SEPERATOR ;//app/db/table/index-count
    public static final String COLUMN_MAPPING_FILE = SEPERATOR + "meta" + SEPERATOR + "column-mapping.bdb";
    public static final String GLOBAL_DELETE_FOLDER = BSQL_BASE_FOLDER + "global-del" + SEPERATOR;
    public static final String GLOBAL_LIVE_FOLDER = BSQL_BASE_FOLDER + "global-live" + SEPERATOR;
    public static final String CONFIF_FILE = BSQL_BASE_FOLDER + "config.json";
    public static final String SERVER_STATUS_FILE = BSQL_BASE_FOLDER + "status.conf";
    public static final String MANIFEST_FILE_NAME = "db-code.mf";
    public static final String SYSTEM_DB = ".systemdb";
    public static final String SYSTEM_DB_FOLDER = BSQL_BASE_FOLDER + SYSTEM_DB + SEPERATOR;
    public static final String RESOURCES = "resources";
    public static final String INITIAL_CREDENTIALS_FILE = BSQL_BASE_FOLDER + SEPERATOR  + "root-pass.txt";

    public static final int STORAGE_VERSION = 4;
    // Map Reduce Related Constants
    public static final String MAP_REDCUCE_STATUS_FILE_NAME = "mapreduce-status.log";
    public static final String MAP_REDUCE_HISTORY_FILE_NAME = "mapreduce-history.log";
    // Folder where all the Map Reduce Job output is stored
    public static final String MAP_REDUCE_FOLDER = SEPERATOR + "map-reduce" + SEPERATOR;

    /* Tableau configuration */
    public static final String TABLEAU_SDK_LOG_DIR = ""; //logs folder for Tableau SDK logs
    public static final String TABLEAU_SDK_TEMP_DIR = ""; //temp folder for Tableau SDK to save intermittent files during on-going operations, like creating a TDE.
    public static final String TABLEAU_LD_LIBRARY_PATH_ENV_VARIABLE_NAME = "LD_LIBRARY_PATH";
    public static final String TABLEAU_LD_LIBRARY_PATH = "install-dir/lib[64]/tableausdk";
    public static final String TABLEAU_CONFIG_FILENAME = "tableau.properties";
    public static final String TABLEAU_SCHEMA_FILENAME = "ts-api_2_5.xsd.xml";
    public static final String TABLEAU_TDE_FILENAME = "tableau.tde";
    public static final String TABLEAU_AUTO_PUBLISH_CONFIG_TABLE = "tableau_auto_publish";

    /* FTP server configuration */
    public static final String FTP_USER_CREDENTIALS_FOLDER = ".ftpusers";
    public static final String FTP_USER_CREDENTIALS_FILE = "ftpusers.properties";

    /* OpenNLP configuration */
    public static final String NLP_PRELOADED_MODELS_FOLDER = "opennlp-models";
    public static final String NLP_LANG_DETECT_FOLDER = "lang-detect";
    public static final String NLP_LANG_DETECT_FILENAME = "langdetect-183.bin";
    public static final String NLP_SENTENCE_DETECT_FOLDER = "sentence-detect";
    public static final String NLP_TOKENIZER_FOLDER = "tokenizer";
    public static final String NLP_SETENCE_DETECT_LANGUAGE_FILENAME = "-sent.bin"; //actual name will be en-sent.bin

    /* Spam detection configuration */
    public static final String SPAM_DATA_FOLDER = BSQL_BASE_FOLDER + RESOURCES + SEPERATOR + "spam-dataset" + SEPERATOR;
    public static final String SPAM_HAM_KEYWORDS_FILE = "hamKeywords.txt";
    public static final String SPAM_LING_MESSAGES_FOLDER = "lingMessages";
    public static final String SPAM_KEYWORDS_FILE = "spamKeywords.txt";
    public static final String SPAM_MESSAGES_FOLDER = "spamMessages";
    public static final String SPAM_TEST_MESSAGES_FOLDER = "testMessages";


    /* License files */
//    public static final String NODE_LICENSE_FILE = BSQL_BASE_FOLDER + License.getNodeId() + ".lic";
    public static final String NODE_LICENSE_FILE = BSQL_BASE_FOLDER + "default.lic";
    public static final String LICENSE_PUBLIC_KEY_FILE_NAME = "public.key";
    public static final String LICENSE_PUBLIC_KEY_FOLDER = "lic";
    public static final String LICENSE_PUBLIC_KEY_FILE = BSQL_BASE_FOLDER + RESOURCES + SEPERATOR
            + LICENSE_PUBLIC_KEY_FOLDER + SEPERATOR + LICENSE_PUBLIC_KEY_FILE_NAME;
}
