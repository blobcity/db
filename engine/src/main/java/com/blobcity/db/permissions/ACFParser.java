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

package com.blobcity.db.permissions;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This bean is used to parse the ACL files which contain the access permission for various users
 * 
 * @author sanketsarang
 */
@Component
public class ACFParser {
    private static final Logger logger = LoggerFactory.getLogger(ACFParser.class);
    
    
    
    public List<String> getDataPermissions(final String fileName){
        return new ArrayList<>();
    }
    
    
    public List<String> getSchemaPermissions(final String fileName){
        return new ArrayList<>();
    }
    
    
    public List<String> getSystemPermissions(final String fileName){
        return new ArrayList<>();
    }
    
    public List<String> getUserPermissions(final String fileName){
        return new ArrayList<>();
    }
    
    
    public List<String> readAllLines(final String fileName) throws OperationException{
        Path path = FileSystems.getDefault().getPath(fileName);
        List<String> allLines;
        try{
            allLines = Files.readAllLines(path);
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error in reading ACL File");
        }
        return allLines;
    }
    
    
    public Integer getPriority(final String fileName){
        return -1;
    }
    
}
