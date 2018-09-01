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


//import com.blobcity.db.constants.BSql;
//import com.blobcity.db.exceptions.ErrorCode;
//import com.blobcity.db.exceptions.OperationException;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.json.JSONObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Driver bean for running map-reduce jobs inside Kraken which handles all the configurations required for running a
 * map reduce job
 *
 * @author sanketsarang
 */
@Component
@Deprecated //for map-reduce jar failing docker security check
public class MapReduceDriver {
//    private static final Logger logger = LoggerFactory.getLogger(MapReduceDriver.class);
//
//    @Autowired
//    private JarManager jarManager;
//    @Autowired
//    private MapReduceJobManager jobManager;
//
//    /* jobId -> Job Object */
//    /* jobId/JobName: usually defined by MapReducerExecutor, a timestamp */
//    /* job Object: given by Hadoop */
//    private static final Map<String, Job> mapReduceJobMap = new HashMap<>();
//
//    /**
//     * Driver function for running a Map Reduce job. This function prepares all the pre-conditions and configurations
//     *
//     * @param jobName: name of map-reduce job
//     * @param database: database name
//     * @param dataTable: name of table from which data needs to be processed
//     * @param mapperClass: mapper class file
//     * @param reducerClass: reducer class file
//     * @param outputTable: the name of table where the output is to be stored
//     * @throws com.blobcity.db.exceptions.OperationException
//     */
//    public void runMapReduce(String jobName, String database, String dataTable,
//            Class mapperClass, Class reducerClass, String outputTable) throws OperationException{
//        String jarName = mapperClass.getSimpleName() + "-" + reducerClass.getSimpleName() + ".jar";
//        jobManager.setStatus(jobName, MRJobStatus.RUNNING);
//
//        try {
//            /* List of mapper and reducer files */
//            List<String> mapReduceFiles = new ArrayList<>();
//            mapReduceFiles.add(mapperClass.getName());
//            mapReduceFiles.add(reducerClass.getName());
//
//            //TODO: this creates a new jar every time. To be fixed in JarManager
//            jarManager.createJar(database, mapReduceFiles, jarName);
//            String deployPath = BSql.BSQL_BASE_FOLDER + database + BSql.DB_HOT_DEPLOY_FOLDER ;
//
//
//            // Hadoop Configuration Settings
//            /* Although these configurations are present in file HadoopConfig.coreSiteFileName but I am afraid to remove them */
//            /* Mess with these on you own risk */
//            Configuration conf = new Configuration();
//            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//            conf.set("fs.default.name", "file:///");
//
//            // these resources should be dynamic (use them inside the project)
//            conf.addResource(new Path(HadoopConfig.configLocation + HadoopConfig.coreSiteFileName));
//            conf.addResource(new Path(HadoopConfig.configLocation + HadoopConfig.mapRedFileName));
//            conf.set("tmpjars", deployPath + jarName);
//
//
//            // Hadoop Input, output settings
//            FileSystem fs = FileSystem.getLocal(conf);
//            /* Path to input data (path to the table on which map reduce is to be performed) */
//            String inputPathStr = BSql.BSQL_BASE_FOLDER + database + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME + BSql.SEPERATOR + dataTable + BSql.DATA_FOLDER;
//            /* path to Map Reduce job output */
//            String outputPathStr = BSql.BSQL_BASE_FOLDER + database + BSql.MAP_REDUCE_FOLDER + jobName;
//
//            Path inputPath=fs.makeQualified(new Path(inputPathStr));
//            Path outputPath=fs.makeQualified(new Path(outputPathStr));
//
//            // Hadoop Job settings
//            Job job = new Job(conf, jobName);
//            job.setMapperClass(mapperClass);
//            job.setReducerClass(reducerClass);
//
//            job.setMapOutputKeyClass(HadoopConfig.mapOutputKeyClass);
//            job.setMapOutputValueClass(HadoopConfig.mapOutputValueClass);
//
//            job.setOutputKeyClass(HadoopConfig.outputKeyClass);
//            job.setOutputValueClass(HadoopConfig.outputValueClass);
//
//            FileInputFormat.addInputPath(job, inputPath);
//            FileOutputFormat.setOutputPath(job, outputPath);
//            mapReduceJobMap.put(jobName, job);
//            job.waitForCompletion(true);
//
//            jobManager.setStatus(jobName, MRJobStatus.COMPLETED);
//
//            // Populate table with the data provided.
//            //TODO: add support for auto-importing output data once job is completed.
//            if(outputTable != null && !outputTable.isEmpty()){
//                // NOT Supported yet
//            }
//        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
//            jobManager.setStatus(jobName, MRJobStatus.ERROR);
//            logger.error(ex.getMessage(), ex);
//            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
//        }
//    }
//
//    /**
//     * Cancel a running Map Reduce job
//     *
//     * @param jobId: id of the Map Reduce job
//     * @throws OperationException : if there is some error in killing the job
//     */
//    public void cancelJob(String jobId) throws OperationException{
//        try {
//            Job currJob = mapReduceJobMap.get(jobId);
//            if(currJob == null){
//               throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given job doesn't exist in this run. Perhaps it will completed in previous run.");
//            }
//            if(currJob.isComplete()){
//                throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given job has been already completed");
//            }
//            currJob.killJob();
//            jobManager.setStatus(jobId, MRJobStatus.CANCELLED);
//        } catch (IOException ex) {
//            logger.error(null, ex);
//            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
//        }
//    }
//
//    /**
//     * Get the progress information about a job
//     *
//     * @param jobId: id of the mapReduce job
//     * @return: String object from a JSONObject containing the information
//     */
//    public JSONObject getJobProgress(String jobId) throws OperationException{
//        JSONObject infoJSON = new JSONObject();
//        infoJSON.put("id", jobId);
//        try {
//            Job job = mapReduceJobMap.get(jobId);
//            if(job == null)
//                throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given job doesn't exist");
//            infoJSON.put("setup", job.setupProgress()*100);
//            infoJSON.put("map", job.mapProgress()*100);
//            infoJSON.put("reduce", job.reduceProgress()*100);
//            infoJSON.put("isComplete", job.isComplete());
//            infoJSON.put("isSuccessful", job.isSuccessful());
//        } catch (IOException ex) {
//            logger.error("Error in getting info for Map Reduce job: " + jobId, ex);
//            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
//        }
//        return infoJSON;
//    }
    
}
