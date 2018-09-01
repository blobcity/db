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

import com.blobcity.db.code.LoaderStore;
import com.blobcity.db.code.RestrictedClassLoader;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.util.Date;
import javax.annotation.PreDestroy;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Bean for executing map reduce jobs inside Database.
 * 
 * @author sanketsarang
 */
@Component
@Deprecated //for map-reduce jar failing docker security check
public class MapReduceExecutor {
//    private static final Logger logger = LoggerFactory.getLogger(MapReduceExecutor.class);
//
//    @Autowired
//    private MapReduceDriver mapReduceDriver;
//    @Autowired
//    private LoaderStore loaderStore;
//    @Autowired
//    private MapReduceJobManager jobManager;
//
//    /**
//     * Start a new Map Reduce job inside database
//     *
//     * @param datastore: name of dsSet where input table
//     * @param dataTable: name of table where the input data is present
//     * @param mapperName: Mapper class name (full name including package)
//     * @param reducerName: Reduce class name (full name)
//     * @param outputTable: name of table where output data should be imported once job is complete (Must be in the same dsSet as dataTable)
//     *                   This feature is not yet implemented as it is dependent on in-memory tables.
//     * @return: job Id which can be used to uniquely identify the job
//     * @throws OperationException
//     */
//    public String startJob(String datastore, String dataTable, String mapperName, String reducerName, String outputTable) throws OperationException{
//        RestrictedClassLoader blobCityLoader = loaderStore.getNewLoader(datastore);
//        Class mapperClass, reducerClass;
//        try {
//            /* Load Mapper, Reducer and other class if required in classpath */
//            mapperClass = blobCityLoader.loadClass(mapperName);
//            reducerClass = blobCityLoader.loadClass(reducerName);
//            //TODO: create jar here with other user-provided files support
//        } catch (ClassNotFoundException ex) {
//            logger.error("Error in loading Map Reduce classes: " + mapperName + ", " + reducerName, ex);
//            throw new OperationException(ErrorCode.CLASS_LOAD_ERROR, "No such Mapper/Reducer classes found");
//        }
//
//        //TODO : decide where to create the jar, here or in driver
//        String mrJobId = System.currentTimeMillis() + "";
//        MRJobInfo info = new MRJobInfo(mrJobId, new Date().toString(), datastore, dataTable, mapperName, reducerName, outputTable);
//        jobManager.registerNewJob(mrJobId, MRJobStatus.QUEUED, info);
//
//        Thread t = new Thread(() -> {
//            try {
//                mapReduceDriver.runMapReduce(mrJobId, datastore, dataTable, mapperClass, reducerClass, outputTable);
//            } catch (OperationException ex) {
//                jobManager.setStatus(mrJobId, MRJobStatus.ERROR, true);
//            }
//        });
//        t.start();
//        return mrJobId;
//    }
//
//    /**
//     * Cancel a running Map Reduce job
//     *
//     * @param jobId: map reduce job id given when job was started
//     * @throws OperationException: if
//     *      1. job does not exist
//     *      2. job is already completed
//     */
//    public void cancelJob(String jobId) throws OperationException{
//        if(!jobManager.jobExists(jobId)) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such job isPresent");
//        }
//        if(jobManager.isJobComplete(jobId)) {
//            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given job is already complete");
//        }
//        mapReduceDriver.cancelJob(jobId);
//        jobManager.setStatus(jobId, MRJobStatus.CANCELLED);
//    }
//
//    /**
//     * Get progress of a Map Reduce job
//     *
//     * @param jobId: map reduce job Id
//     * @return JSONObject containing all the information
//     * @throws OperationException:
//     *      1. if job doesn't exist
//     */
//    public JSONObject getProgress(String jobId) throws OperationException{
//        if(!jobManager.jobExists(jobId))
//            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given job doesn't exist");
//        return mapReduceDriver.getJobProgress(jobId);
//    }
//
//    /**
//     * this will set the status of currently running jobs as canceled when the database is stopped somehow
//     */
//    @PreDestroy
//    public void beforeDestroy(){
//        //TODO: this is not working
//        jobManager.getActiveJobs().stream().forEach((jobId) -> {
//            try {
//                logger.debug("Cancelling Map Reduce job: " + jobId);
//                jobManager.setStatus(jobId, MRJobStatus.CANCELLED);
//            } catch (OperationException ex) {
//                logger.error("Error in canceling Map Reduce job: " + jobId, ex);
//            }
//        });
//    }
}
