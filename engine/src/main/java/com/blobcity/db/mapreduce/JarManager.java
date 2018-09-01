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
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * This class is used to maintain information about various jar that were dynamically created for map-reduce.
 * Dynamically generated Jar will contain the following:
 *      Mapper.class
 *      Reducer.class
 *
 * This ensures that we don't create the jar every time a map-reduce operation is requested.
 * For the first time, a jar is created which is used for the further mapReduce operations involving the
 * same mapper and reducer files.
 *
 * Note: this doesn't check for the changes. That part should be implemented in future.
 * 
 * @author sanketsarang
 */
@Component
public class JarManager {    
    private static final Logger logger = LoggerFactory.getLogger(JarManager.class);
    
    private static final String baseDir = BSql.BSQL_BASE_FOLDER;

    public static boolean jarExists(String database, String jarName){
        String fullPath = baseDir + database + BSql.SEPERATOR + jarName; 
        return new File(fullPath).exists();
    }
    
    /**
     * Creates a jar file in the db-deploy-hot folder of given folder with given file list
     * 
     * @param database: database name
     * @param files: files to be added to the jar with complete name (including package also).
     * @param jarName: name of jar to be created
     * @throws OperationException: if some internal error occurred during creation
     */
    public void createJar(String database, List<String> files, String jarName) throws OperationException{
        
        if(jarExists(database, jarName)) return ;       
        String path = baseDir + database + BSql.DB_HOT_DEPLOY_FOLDER ;
        
        int BUFFER_SIZE = 10240;
        byte buffer[] = new byte[BUFFER_SIZE];
                
        try {
            FileOutputStream fout = new FileOutputStream(path+jarName);
            JarOutputStream jout = new JarOutputStream(fout, new Manifest());
            // adding mapper and reducer classes to jar now
            for (String file : files) {
                if(!file.contains(".class")) file = file + ".class";
                JarEntry jarAdd = new JarEntry(file);
                jout.putNextEntry(jarAdd);
                try ( FileInputStream in = new FileInputStream(path + file)) {
                    while (true) {
                        int nRead = in.read(buffer, 0, buffer.length);
                        if (nRead <= 0) break;
                        jout.write(buffer, 0, nRead);
                    }
                    in.close();
                }
            }
            
            jout.close();
            fout.close();
        } catch (FileNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
    
    /**
     * this will delete the jar from the hard drive
     * 
     * @param database: name of database 
     * @param jarName: name of jar file
     */
    public static void removeJar(String database, String jarName){
        String fullPath = baseDir + database + BSql.SEPERATOR + jarName;
        if(new File(fullPath).exists()){
            new File(fullPath).delete();
        }
    }
    
}
