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

import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.OperationException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This bean stores and manages information about various Map Reduce jobs.
 * 
 * @author sanketsarang
 */
@Component
@Deprecated
public class MapReduceJobManager {
    private static final Logger logger = LoggerFactory.getLogger(MapReduceJobManager.class);
    
    /* jobId -> jobStatus */
    private static final Map<String, MRJobStatus> jobStatusMap = new HashMap<>();
    /* jobId->jobInfo */
    private static final Map<String, MRJobInfo> jobInfoMap = new HashMap<>();
    /* whether the previous jobs from history has been loaded or not */
    /* This only loads the information and doesn't restart/resume jobs */
    private static boolean historyLoaded  = false;
    
//    @PostConstruct
    public void init(){
        loadHistory();
    }
    
    /**
     * Load previous Map Reduce job from the file on disk
     * Happens on every database boot up.
     * This is done so that users can still fetch the information about their previous jobs.
     */
    public void loadHistory(){
        /* Path to Map Reduce History File */
        String historyFilePath = BSql.BSQL_BASE_FOLDER + BSql.MAP_REDUCE_HISTORY_FILE_NAME;
        loadInformation(historyFilePath);

        /* Path to Map Reduce Status File */
        String statusFilePath = BSql.BSQL_BASE_FOLDER + BSql.MAP_REDCUCE_STATUS_FILE_NAME;
        loadStatus(statusFilePath);
        historyLoaded = true;

//        logger.debug("Map Reduce History loaded successfully");
    }

    /**
     * Load the information about previous Map Reduce jobs
     *
     * @param historyFilePath: path to history file
     */
    public void loadInformation(final String historyFilePath){
        try{
            BufferedReader br = new  BufferedReader(new FileReader(historyFilePath));
            String line;
            String[] jobInfo;
            while((line = br.readLine()) != null){
                jobInfo = line.split(",");
                jobInfoMap.put(jobInfo[0], new MRJobInfo(line));
            }
        } catch (IOException ex) {
//            logger.error("Error in Reading Map Reduce File History File:  " + historyFilePath, ex);
        }
    }

    /**
     * Load status information about previous Map Reduce jobs
     * Line Structure: jobId, Text TimeStamp, String Status (CSV)
     *
     * @param statusFilePath: path to status File
     */
    public void loadStatus(final String statusFilePath){

        try{
            BufferedReader br = new BufferedReader(new FileReader(statusFilePath));
            String line;
            String[] jobStatus;
            while((line = br.readLine()) != null){
                jobStatus = line.split(",");
                jobStatusMap.put(jobStatus[0], MRJobStatus.fromString(jobStatus[2]));
            }
        } catch (IOException ex) {
//            logger.error("Error in Reading Map Reduce File Status File:  " + statusFilePath, ex);
        }
    }

    /**
     * Register a new Map Reduce job
     *
     * @param jobId: job Id
     * @param status: status of job
     * @param jobInfo: information about job
     * @throws OperationException: if job already exists
     */
    public void registerNewJob(String jobId, MRJobStatus status, MRJobInfo jobInfo) throws OperationException{
        if(jobStatusMap.containsKey(jobId)){
            throw new OperationException(null, "Given job already exists");
        }
        jobStatusMap.put(jobId, status);
        jobInfoMap.put(jobId, jobInfo);
        dumpToLogFile(jobId, jobInfo.toString());
        dumpStatus(jobId, status.getStatus());
    }

    /**
     * Set status of Map Reduce job
     *
     * @param jobId: job Id
     * @param status: new status of the job
     * @throws OperationException: if no such job exists
     */
    public void setStatus(String jobId, MRJobStatus status) throws OperationException{
        if(!jobStatusMap.containsKey(jobId)){
            throw new OperationException(null, "No such job exists");
        }
        jobStatusMap.put(jobId, status);
        dumpStatus(jobId, status.getStatus());
    }
    
    /**
     * Set status of a job without checking whether job exists or not
     * This is required when the database is going for a shut down
     *
     * @param jobId: job Id
     * @param status: status of the job
     * @param override : whether to override the default job exists check
     */
    public void setStatus(String jobId, MRJobStatus status, boolean override){
        if(override){
            jobStatusMap.put(jobId, status);
            dumpStatus(jobId, status.getStatus());
        }
    }

    /**
     * Get Status of a Map Reduce job
     *
     * @param jobId: job Id
     * @return: Status of the job
     * @throws OperationException: if no such job exists
     */
    public MRJobStatus getStatus(String jobId) throws OperationException{
        if(!jobStatusMap.containsKey(jobId)){
            throw new OperationException(null, "No such job exists");
        }
        return jobStatusMap.get(jobId);
    }

    /**
     * Get Info about a Map Reduce Job
     *
     * @param jobId: job Id
     * @return: JobInfo Object
     */
    public MRJobInfo getJobInfo(String jobId){
        if(!jobInfoMap.containsKey(jobId)) return null;
        else{
            return jobInfoMap.get(jobId);
        }
    }
    
    /**
     * Append the map reduce job information in the history file
     * Format: jobId, time, database, dataTable, mapperClass, reducerClass, outputTable (csv)
     * 
     * @param jobId: job Id
     * @param jobData: info about job in String (CSV)
     */
    private void dumpToLogFile(String jobId, String jobData){
        String historyFilePath = BSql.BSQL_BASE_FOLDER + BSql.MAP_REDUCE_HISTORY_FILE_NAME;
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(historyFilePath, true))) {
            bw.append(jobId + "," + jobData);
            bw.newLine();
            bw.close();
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        } 
    }
    
    /**
     * Append the job status information in the status file
     * Format: jobId, time, status (csv)
     *
     * @param jobId: job Id
     * @param status: Job status as String
     */
    private void dumpStatus(String jobId, String status){
        String statusFilePath = BSql.BSQL_BASE_FOLDER + BSql.MAP_REDCUCE_STATUS_FILE_NAME;
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(statusFilePath, true))) {
            bw.append(jobId + "," + new Date() + "," + status);
            bw.newLine();
            bw.close();
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        } 
    }

    /**
     * Get Map Reduce History
     *
     * @return: List of all Map Reduce jobs
     */
    public List<String> getHistory(){
        List<String> allJobs = new ArrayList<>();
        for(String jobId:jobInfoMap.keySet()){
            allJobs.add(jobId + "," + jobInfoMap.get(jobId).toString() + "," + jobStatusMap.get(jobId).getStatus());
        }
        return allJobs;
    }

    /**
     * Whether a given job exists
     *
     * @param jobId: job Id
     * @return
     */
    public boolean jobExists(String jobId){
        return jobInfoMap.containsKey(jobId);
    }

    /**
     * Whether a given Map Reduce job is complete or not
     *
     * @param jobId: job Id
     * @return: true if complete, false otherwise
     */
    public boolean isJobComplete(String jobId){
        if(!jobInfoMap.containsKey(jobId)) return false;
        if(!jobStatusMap.containsKey(jobId)) return false;
        return jobStatusMap.get(jobId).equals(MRJobStatus.COMPLETED);
    }
    
    /**
     * returns list of running jobs only
     * 
     * @return List of Ids for jobs which are currently running
     */
    public List<String> getRunningJobs(){
        List<String> runningJobs = new ArrayList<>();
        jobStatusMap.keySet().stream().filter((jobId) -> 
                (jobStatusMap.get(jobId).equals(MRJobStatus.RUNNING))).forEach((jobId) -> {
            runningJobs.add(jobId);
        });
        return runningJobs;
    }
    
    /**
     * returns the list of active jobs(running or queued)
     *
     * @return : List of ids for jobs which are active
     */
    public List<String> getActiveJobs(){
        List<String> activeJobs = new ArrayList<>();
        jobStatusMap.keySet().stream().filter((jobId) -> 
                (jobStatusMap.get(jobId).equals(MRJobStatus.RUNNING) ||jobStatusMap.get(jobId).equals(MRJobStatus.QUEUED) )).forEach((jobId) -> {
            activeJobs.add(jobId);
        });
        return activeJobs;
    }
}
