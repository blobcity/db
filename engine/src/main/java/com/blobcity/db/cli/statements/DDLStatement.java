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

package com.blobcity.db.cli.statements;

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.ReplicationType;
import com.blobcity.db.schema.SchemaBuilder;
import com.blobcity.db.schema.TableType;
import java.io.IOException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class DDLStatement {

    @Autowired
    private BSqlCollectionManager tableManager;

    public String createTable(String[] elements) throws OperationException, IOException {
        final String databaseAndTable = elements[1];
        if (!databaseAndTable.contains(".")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Datstore and collection should be specified in format: dsName.collectionName");
        }

        final String database = databaseAndTable.substring(0, databaseAndTable.indexOf("."));
        final String table = databaseAndTable.substring(databaseAndTable.indexOf(".") + 1, databaseAndTable.length());

        if (tableManager.exists(database, table)) {
            return "A collection with the given name is already present";
        }

        SchemaBuilder builder = SchemaBuilder.newDefault();
        CreateTableArgumentsParser parser = new CreateTableArgumentsParser(elements);
        parser.applyToSchema(builder);

        tableManager.createTable(database, table, builder.toJson());
        return "Collection successfully created";
    }
}

class CreateTableArgumentsParser {

    private static final Options options;
    private static final Logger logger = LoggerFactory.getLogger(DDLStatement.class.getName());

    static {
        options = new Options();
        options.addOption("t", true, "table type [on-disk,in-memory,in-memory-non-durable]");
        options.addOption("flexible", true, "flexible schema");
        options.addOption("r", true, "replication type [distributed,mirrored]");
        options.addOption("rf", true, "replication factor [>= 0]");
        options.addOption("df", true, "data file name used for schema to find columns for in memory tables, used only for table type in-memory");
        options.addOption("dfh", true, "if data file has a header [true/false], used only for table type in-memory");
        options.addOption("dft", true, "data file type [XML/JSON/CSV/HadoopOutput], used only for table type in memory");
        options.addOption("dfs", true, "data file separator [space/comma/tab], used only for table type in memory");
        options.addOption("dfsl", true, "skip n lines in data file, some data files start with comments, used only for table type in-memory");
    }

    private final TableType tableType;
    private final ReplicationType replicationType;
    private final Integer replicationFactor;
    private final Boolean flexibleSchema;
    private final String dataFileForInMemoryTableSchema; // use this file name to find the columns for in memory table
    private final Boolean dataFileHasHeader; // if the first row contains names of columns, default false
    private final Integer dataFileLinesToSkip; // lines to skip, some data files start with comments, default 0
    private final String dataFileType;
    private final String dataFileSeparator;

    public CreateTableArgumentsParser(String[] args) throws OperationException {
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            /* Set table type if specified */
            if (cmd.hasOption("t")) {
                tableType = TableType.fromString(cmd.getOptionValue("t"));
            } else {
                tableType = null;
            }

            /* Set flexible schema if specified */
            if (cmd.hasOption("flexible")) {
                flexibleSchema = Boolean.getBoolean(cmd.getOptionValue("flexible"));
            } else {
                flexibleSchema = true; //automatically set when parameter not specified
            }

            /* Set replication type if specified */
            if (cmd.hasOption("r")) {
                replicationType = ReplicationType.fromString(cmd.getOptionValue("r"));
            } else {
                replicationType = null;
            }

            /* Set replication factor if specified */
            if (cmd.hasOption("rf")) {
                replicationFactor = Integer.parseInt(cmd.getOptionValue("rf"));
                if (replicationFactor < 0) {
                    throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Replication factor must have value greater than 0");
                }
            } else {
                replicationFactor = 0; //default value
            }

            // for in-memory tables, see if  data file is specified
            if ((cmd.hasOption("df")) && ((tableType == TableType.IN_MEMORY) || (tableType == TableType.IN_MEMORY_NON_DURABLE))) {
                dataFileForInMemoryTableSchema = cmd.getOptionValue("df");
                if (!cmd.hasOption("dft")) {
                    throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "data file type must be present for in memory table schema");
                }
                logger.debug("tableType: " +tableType + ", dafafile: " +dataFileForInMemoryTableSchema);
                if (cmd.hasOption("dft")) {
                    dataFileType = cmd.getOptionValue("dft");
                    if(!dataFileType.equalsIgnoreCase("CSV") && !dataFileType.equalsIgnoreCase("XML") &&
                            !dataFileType.equalsIgnoreCase("JSON") && !dataFileType.equalsIgnoreCase("HadoopOutput")) {
                        throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "unknown data file type for in memory table");
                    }
                }
                else {
                    dataFileType = null;
                }
                if (cmd.hasOption("dfh")) {
                    dataFileHasHeader = cmd.getOptionValue("dfh").equals("true");
                } else {
                    dataFileHasHeader = false;
                }
                if (cmd.hasOption("dfs")) {
                    switch (cmd.getOptionValue("dfs")) {
                        case "space":
                            dataFileSeparator = "space";
                            break;
                        case "comma":
                            dataFileSeparator = "comma";
                            break;
                        case "tab":
                            dataFileSeparator = "tab";
                            break;
                        default:
                            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "unknown data file separator for in memory table");
                    }
                } else {
                    dataFileSeparator = null;
                }
                if (cmd.hasOption("dfsl")) {
                    dataFileLinesToSkip = Integer.parseInt(cmd.getOptionValue("dfsl"));
                } else {
                    dataFileLinesToSkip = 0;
                }
            } else {
                dataFileForInMemoryTableSchema = null;
                dataFileHasHeader = false;
                dataFileLinesToSkip = 0;
                dataFileType = null;
                dataFileSeparator = null;
            }
        } catch (ParseException | NumberFormatException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "parsing of cli query parameters failed. " + ex.getMessage());
        }
    }

    public void applyToSchema(SchemaBuilder builder) {
        if (tableType != null) {
            builder.type(tableType);
        }

        if (replicationType != null) {
            builder.replication(replicationType);
        }

        if (replicationFactor != null) {
            builder.replication(replicationFactor);
        }

        if (flexibleSchema != null) {
            builder.flexibleSchema(flexibleSchema);
        }
         
        if(dataFileForInMemoryTableSchema != null) {
            builder.dataFile(dataFileForInMemoryTableSchema);
        }
        
        if(dataFileHasHeader != null) {
            builder.dataFileHasHeader(dataFileHasHeader);
        }
        
        if(dataFileLinesToSkip != null) {
            builder.dataFileLinesToSkip(dataFileLinesToSkip);
        }
        
        if(dataFileType != null) {
            builder.dataFileType(dataFileType);
        }
        
        if(dataFileSeparator != null) {
            builder.dataFileSeparator(dataFileSeparator);
        }
    }
}
