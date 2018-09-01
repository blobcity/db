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

/**
 * This class stores various information about a Map Reduce Job
 *
 * @author sanketsarang
 */
public class MRJobInfo {

    private String jobId;
    private String time;
    private String database;
    private String dataTable;
    private String mapperClass;
    private String reducerClass;
    private String outputTable;

    public MRJobInfo(String jobId, String time, String db, String table1, String map, String reducer, String output) {
        this.jobId = jobId;
        this.time = time;
        this.database = db;
        this.dataTable = table1;
        this.mapperClass = map;
        this.reducerClass = reducer;
        this.outputTable = output;
    }

    /**
     * Get the information of a job from a String (CSV)
     *
     * @param jobData: String (CSV)
     */
    public MRJobInfo(String jobData){
        String[] elements= jobData.split(",");
        this.jobId = elements[0];
        this.time = elements[1];
        this.database = elements[2];
        this.dataTable = elements[3];
        this.mapperClass = elements[4];
        this.reducerClass = elements[5];
        this.outputTable = elements[6];
    }
    
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDataTable() {
        return dataTable;
    }

    public void setDataTable(String dataTable) {
        this.dataTable = dataTable;
    }

    public String getMapperClass() {
        return mapperClass;
    }

    public void setMapperClass(String mapperClass) {
        this.mapperClass = mapperClass;
    }

    public String getReducerClass() {
        return reducerClass;
    }

    public void setReducerClass(String reducerClass) {
        this.reducerClass = reducerClass;
    }

    public String getOutputTable() {
        return outputTable;
    }

    public void setOutputTable(String outputTable) {
        this.outputTable = outputTable;
    }
    
    @Override
    public String toString(){
        return time+","+database+","+dataTable+","+mapperClass+","+reducerClass+","+outputTable;
    }

    public String getTime() {
        return time;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setTime(String time) {
        this.time = time;
    }
    
}
