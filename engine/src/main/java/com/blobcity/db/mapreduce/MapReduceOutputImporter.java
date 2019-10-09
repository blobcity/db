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

package com.blobcity.db.mapreduce;

import au.com.bytecode.opencsv.CSVReader;
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.ClusterDataManager;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import com.blobcity.db.schema.AutoDefineTypes;
import com.blobcity.db.schema.IndexTypes;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.beans.SchemaManager;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This bean handles the Hadoop Map reduce output and inserts it into database using the clustering layer
 *
 * @author sanketsarang
 */
@Component
public class MapReduceOutputImporter {
    private static final Logger logger  = LoggerFactory.getLogger(MapReduceOutputImporter.class);

    @Autowired
    private ClusterDataManager clusterDataManager;
    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private BSqlCollectionManager tableManager;
    @Autowired
    private SchemaManager schemaManager;
    @Autowired
    private MapReduceJobManager mapReduceJobManager;
    
     
    /**
     * returns the schema
     * if no schema is there, it will create a new schema with only primary column
     * if no primary column, new primary column with UID autoDefine
     * @param app
     * @param table
     * @return false if no schema is defined other than Primary Column
     *          true otherwise
     * @throws OperationException 
     */
    public boolean ensureSchemaPresent(String app, String table) throws OperationException{
        Schema schema = null;
        try{
            schema = schemaManager.readSchema(app, table);
            // create a primary column
            if(schema.getPrimary() == null || schema.getPrimary().isEmpty() ){
                schemaManager.insertPrimaryColumn(app, table);
            }
            return schema.getColumnMap().size() < 1;
        }catch(OperationException ex){
            // no schema found
            if(ex.getErrorCode() == ErrorCode.SCHEMA_FILE_NOT_FOUND){
                schemaManager.insertPrimaryColumn(app, table);
                return false;
            }
            else{
                throw ex;
            }
        }
    }
    
    /**
     * this will update the schema with the new given columns 
     * (will retain the previous columns also)
     * 
     * @param app
     * @param table
     * @param columns
     * @throws OperationException 
     */
    public void updateSchema(String app, String table, String[] columns) throws OperationException {
        Schema schema = schemaManager.readSchema(app, table);
        // by default the column type is set as string
        // TODO: in future, either auto detect or user provided
        FieldType fieldType = FieldTypeFactory.fromString("string");
        for(String col: columns){
            // add the new column only if it is not present
            if( !schema.getColumnMap().containsKey(col)){
                tableManager.addColumn(app, table, col, fieldType, AutoDefineTypes.NONE, IndexTypes.NONE);
            }
        }
    }
    
    /**
     * determine the structure of given file (CSV or JSON)
     * 
     * @param filePath: absolute path of file
     * @return: file type
     * @throws OperationException 
     */
    public String determineFileType(String filePath) throws OperationException{
        try{
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line = br.readLine();
            //TODO proper regex here
            if(line.split(",").length > 1) return "csv";
            else if(line.startsWith("{") && line.endsWith("}")) return "json";
            else return "unknown";
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, ex.getMessage());
        }
    }
     
    /**
     * this assumes that the schema is already there and 
     * either the primary column/s is/are in the column array or is autoDefined
     * 
     * @param app: database in which data is to be inserted
     * @param table: table in which data is to be inserted 
     * @param filePath: full path of CSV file to be imported
     * @param columns: column Mapping of the file
     * @param mappingInFirstLine: whether the first line contains the column mapping  or not.
     * @throws OperationException 
     */
    public void readCSVFile(String app, String table, String filePath, String[] columns, final boolean mappingInFirstLine) throws OperationException{
        int totalRows = 0;
        List<Integer> failedRows = new ArrayList<>();
        int currentLine = 1;
        try{
            CSVReader csvReader = new CSVReader(new FileReader(filePath));
            String[] rowData;
            JSONObject rowJson;

            if(mappingInFirstLine){
                csvReader.readNext();
                currentLine++;
            }
            while((rowData = csvReader.readNext()) != null){
                totalRows++;
                if(rowData.length == columns.length){
                    rowJson = new JSONObject();
                    for(int i=0;i<rowData.length; i++) rowJson.put(columns[i], rowData[i]);
                    logger.debug("inserting: " + rowJson.toString());
//                    clusterDataManager.insert(app, table, rowJson);
                    dataManager.insert(app, table, rowJson);
                }
                else{
                    failedRows.add(currentLine);
                }
                currentLine++;
            }
            csvReader.close();
        } catch (FileNotFoundException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "File "+ filePath + "couldn't be found");
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "IOException in reading file: " + filePath);
        }
        // Return total count and failed rows to user
        logger.debug("Import failed for #" + failedRows.size() + "rows out of " + totalRows);
        logger.debug("Failed Rows are: " + failedRows.toString());
    }
    
    /**
     * reads a JSON file and insert into database
     * 
     * Assumes that Primary column/s is/are either autoDefined or present itself in the JSON data
     * 
     * @param app: database where data is to be inserted
     * @param table: table where data is to be inserted
     * @param filePath: absolute path of file
     * @throws OperationException 
     */
    public void readJsonFile(String app, String table, String filePath) throws OperationException{
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            JSONObject rowJson;
            while( (line = br.readLine()) != null ){
                rowJson = new JSONObject(line);
                clusterDataManager.insert(app, table, rowJson);
            }
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
    
    /**
     * it reads the column mapping of a CSV file
     * which is present in single line.
     * 
     * @param filePath: absolute path of file
     * @return: column mapping of data as defined in the first line.
     * @throws OperationException 
     */
    public String[] readColumnMapping(final String filePath) throws OperationException{
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            line = br.readLine();
            return line.split(",");
        }
        catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, ex.getMessage());
        }
    }
    
    /**
     * this return the list of all default output files generated during a map-reduce job
     * @param jobId: id of map=reduce job
     * @return 
     * @throws com.blobcity.db.exceptions.OperationException if output directory doesn't exist
     */
    public List<String> getOutputFiles(String jobId) throws OperationException{
        String mrDatabase = mapReduceJobManager.getJobInfo(jobId).getDatabase();
        String outputFolder = BSql.BSQL_BASE_FOLDER + mrDatabase + BSql.MAP_REDUCE_FOLDER + jobId;
        
        File outputDir = new File(outputFolder);
        if( !outputDir.exists()){
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Data for the given job doesn't exist");
        }        
        List<String> files = new ArrayList<>();
        File[] allFiles = outputDir.listFiles((File dir, String name) -> name.contains("part") && !name.contains("crc"));
        for(File fl : allFiles){
            files.add(fl.getAbsolutePath());
        }
        if(files.size() < 1){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No output files are found for given job");
        }
        // this is done to make sure the first file in list is the first output file by MR
        Collections.sort(files);
        return files;
    }
    
    /**
     * when output file is either JSON or CSV with first line as column Mapping
     * 
     * @param jobId
     * @param app
     * @param importTable
     * @throws OperationException 
     */
    public void importOutput(String jobId, String app, String importTable) throws OperationException{
        importOutput(jobId, app, importTable, null);
    }

    /**
     * imports the output of a given map-reduce job in database
     * 
     * @param jobId: map-reduce job id
     * @param app: database where data is to be imported
     * @param importTable: table where data is to be imported 
     * @param columnMappingFile: <p> External file where the column mapping is stored.(used in case of CSV only) </p>
     * @throws OperationException 
     */
    public void importOutput(String jobId, String app, String importTable, String columnMappingFile) throws OperationException{
        
        if(!mapReduceJobManager.jobExists(jobId)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given job doesn't exist");
        }
        if(!mapReduceJobManager.isJobComplete(jobId)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given Job was not completed successfully");
        }
        
        MRJobInfo jobInfo = mapReduceJobManager.getJobInfo(jobId);
        String outputFolder = BSql.BSQL_BASE_FOLDER + jobInfo.getDatabase() + BSql.MAP_REDUCE_FOLDER + jobId;
        
        ensureSchemaPresent(app, importTable);
        List<String> outputFiles = getOutputFiles(jobId);
        
        String fileType =  determineFileType(outputFiles.get(0));
        switch (fileType) {
            case "json":
                for(String outputFile : outputFiles)
                    readJsonFile(app, importTable, outputFile);
                break;
            case "csv":
                break;
            case "unknown":
            default:
                throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "This file format is not supported yet");
        }
        
        String[] columnMapping;
        // this means that mapping is in the first file only
        if(columnMappingFile == null || columnMappingFile.isEmpty()){
            columnMapping = readColumnMapping(outputFiles.get(0));
            updateSchema(app, importTable, columnMapping);
            readCSVFile(app, importTable, outputFiles.get(0), columnMapping, false);
            outputFiles.remove(0);
        }
        else{
            columnMapping = readColumnMapping(outputFolder + BSql.SEPERATOR + columnMappingFile);
            updateSchema(app, importTable, columnMapping);
        }
        
        for(String outputFile : outputFiles){
            readCSVFile(app, importTable, outputFile, columnMapping, false);
        }
    }
    
    //TODO : to be put in separate file later.
    public void importCSVFile(final String app, final String table, final String filePath) throws OperationException{
        if(!tableManager.exists(app, table))
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER,"Given database table doesn't exist");
        
        Schema schema = schemaManager.readSchema(app, table);
        //read column Mapping from the file.
        String[] cols = readColumnMapping(filePath);
        
        if(schema.isFlexibleSchema()){
            updateSchema(app, table, cols);
        }
        schema = schemaManager.readSchema(app, table);
        // checking schema now
        for(String col: cols){
            if(schema.getColumn(col) == null){
                throw new OperationException(ErrorCode.INSERT_ERROR, "Pre-defined table columns and file columns don't match."
                        + " Did u create table with flexible schema?");
            }
        }
        // read file now
        readCSVFile(app, table, filePath, cols, true);
    }

}
