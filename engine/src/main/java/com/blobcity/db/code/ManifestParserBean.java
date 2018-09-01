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

package com.blobcity.db.code;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.PathUtil;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This bean reads the Manifest file present in each of the datastores
 * Use this bean to get any section of Manifest file
 *
 * @author sanketsarang
 */
@Component
@Deprecated
public class ManifestParserBean {
    private static final Logger logger = LoggerFactory.getLogger(ManifestParserBean.class);
    
    public List<String> getAll(String datastore) throws OperationException{
        List<String> allCodes = new ArrayList<>();
        allCodes.addAll(getProcedures(datastore));
        allCodes.addAll(getFilters(datastore));
        allCodes.addAll(getTriggers(datastore));
        allCodes.addAll(getMappers(datastore));
        allCodes.addAll(getReducers(datastore));
        return allCodes;
    }
    
    
    public List<String> getProcedures(final String datastore) throws OperationException {
        Path path = FileSystems.getDefault().getPath(PathUtil.codeManifestFile(datastore));
        final List<String> lines;
        try {
            lines = Files.readAllLines(path, Charset.defaultCharset());
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.CODE_MANIFEST_PARSING_ERROR);
        }
        return getSection(lines, ManifestLang.PROCEDURES);
    }

    public List<String> getTriggers(final String datastore) throws OperationException {
        Path path = FileSystems.getDefault().getPath(PathUtil.codeManifestFile(datastore));
        final List<String> lines;
        try {
            lines = Files.readAllLines(path, Charset.defaultCharset());
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.CODE_MANIFEST_PARSING_ERROR);
        }

        return getSection(lines, ManifestLang.TRIGGERS);
    }
    
    public List<String> getFilters(final String datastore) throws OperationException{
        Path path = FileSystems.getDefault().getPath(PathUtil.codeManifestFile(datastore));
        final List<String> lines;
        try {
            lines = Files.readAllLines(path, Charset.defaultCharset());
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.CODE_MANIFEST_PARSING_ERROR);
        }

        return getSection(lines, ManifestLang.FILTERS);
    }
    
    public List<String> getMappers(final String datastore) throws OperationException{
        Path path = FileSystems.getDefault().getPath(PathUtil.codeManifestFile(datastore));
        final List<String> lines;
        try {
            lines = Files.readAllLines(path, Charset.defaultCharset());
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.CODE_MANIFEST_PARSING_ERROR);
        }

        return getSection(lines, ManifestLang.MAPPERS);
    }
    
    public List<String> getReducers(final String datastore) throws OperationException{
        Path path = FileSystems.getDefault().getPath(PathUtil.codeManifestFile(datastore));
        final List<String> lines;
        try {
            lines = Files.readAllLines(path, Charset.defaultCharset());
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.CODE_MANIFEST_PARSING_ERROR);
        }

        return getSection(lines, ManifestLang.REDUCERS);
    }
    
    public List<String> getInterpreters(final String datastores) throws OperationException{
        Path path = FileSystems.getDefault().getPath(PathUtil.codeManifestFile(datastores));
        final List<String> lines;
        try {
            lines = Files.readAllLines(path, Charset.defaultCharset());
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.CODE_MANIFEST_PARSING_ERROR);
        }

        return getSection(lines, ManifestLang.DATAINTERPRETERS);
    }
    
    protected List<String> getSection(final List<String> lines, ManifestLang manifestLang) throws OperationException {
        List<String> procedureList = new ArrayList<>();
        
        //TODO: Put proper REGEX here
        if (!lines.get(0).matches("MANIFEST-VERSION: 1.0")) {
            throw new OperationException(ErrorCode.CODE_MANIFEST_PARSING_ERROR, "Unrecognized manifest file version number");
        }
        
        /* Iterate till procedures section */
        int index = 0;
        while(index < lines.size() && !lines.get(index).equals(manifestLang.getLiteral())) {
            index ++;
        }
        
        /* If last line is reached, means procedures section does not exist in manifest file */
        index ++;
        if(index == lines.size()) {
            return Collections.EMPTY_LIST;
        }
        
        while(index < lines.size() && !ManifestLang.isLiteral(lines.get(index))) {
            if(lines.get(index).trim().isEmpty()) {
                index++;
                continue;
            }
            procedureList.add(lines.get(index++));
        }

        return procedureList;
    }

}
