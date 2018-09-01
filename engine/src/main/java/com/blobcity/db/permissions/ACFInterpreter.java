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

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.beans.SchemaManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * This bean parses ACFs and applies permissions present in there.
 * 
 * @author sanketsarang
 */
@Component
public class ACFInterpreter {
    // entity(user/group) mapped to data hierarchy level mapped to permission value
    private Map<String, Map<String, Integer>> someMap;
    
    @Autowired
    private SchemaManager schemaManager;
    @Autowired @Lazy
    private BSqlCollectionManager collectionManager;
    @Autowired @Lazy
    private BSqlDataManager dataManager;
    
    
    /**
     * this will process the given ACFs in ascending order of priority provided in them
     * and apply them
     * @param permissionLines: list of permission to be parsed
     * @throws com.blobcity.db.exceptions.OperationException : if there is some issue in parsing the lines
     */
    public void startProcessing(List<String> permissionLines) throws OperationException{
        someMap = new HashMap<>();
        parseDataPermissions(permissionLines);
        // convert map into json and insert into database;
    }
    
    /**
     * this will parse and apply all the data permissions provided
     * 
     * @param lines: list of all data permissions
     * @throws OperationException 
     */
    private void parseDataPermissions(List<String> lines) throws OperationException{
        for(String line: lines){
            // remove extra spaces or null elements from the line for a better parsing
            // this will maintain the order of splits as well
            List<String> els = new ArrayList<>(Arrays.asList(line.split(" ")));
            els.removeAll(Collections.singleton(null));
            els.removeAll(Collections.singleton(""));
            String[] params = (String[]) els.toArray( new String[els.size()] );

            // minimum length is 3 for deny permission and 4 for allow permission
            if(params.length < 4) 
               throw new OperationException(ErrorCode.ACF_PARSE_ERROR, "Invalid data permission format for line " + line + "(Minimum Parameters 4)");
            
            // format is as following:
            // user/group accessmode(allow/deny) permission(read/write/all) datalevel(dsSet/collection/column)
            
            if ( !(params[1].toLowerCase().equals("allow")) || !(params[1].toLowerCase().equals("deny")) )
                throw new OperationException(ErrorCode.ACF_PARSE_ERROR, "Invalid accessmode for line " + line);
            parseDataPermissionLine(params[0], params[1], params[2], params[3]);
        }
    }
    
    /**
     * apply a given line of permission in ACL file
     * 
     * @param entity : user/group
     * @param accessmode: allow or deny mode
     * @param permissionlevel: what permission to be applied
     * @param datalevel: data hierarchy level
     * @throws OperationException 
     */
    private void parseDataPermissionLine(final String entity, final String accessmode, 
            final String permissionlevel, final String datalevel) throws OperationException{
        DataPermission permission;
        // parse allow and deny permission separately.
        switch(permissionlevel.toLowerCase() ){
            case "read":
                permission = DataPermission.READ;
                break;
            case "write":
                permission = DataPermission.WRITE;
                break;
            case "all":
                permission = DataPermission.ALL;
                break;
            default:
                throw new OperationException(ErrorCode.ACF_PARSE_ERROR, "No such permission named as " + permissionlevel );
        };
        
        String[] hierarchy = datalevel.split(".");
        switch(hierarchy.length){
            // dsSet level
            case 1:
                applyDataStoreLevelDataPermission(entity, accessmode, permission, datalevel);
                break;
            // collection level
            case 2:
                applyCollectionLevelDataPermission(entity, accessmode, permission, hierarchy[0], hierarchy[1]);
                break;
            // column level
            case 3:
                setDataPermission(entity, accessmode, permission, datalevel);
                break;
        };
    }
    
    /**
     * apply a given data permission at a dataStore level for a given user
     * (this is recursive to column level)
     * 
     * @param entity : user/group
     * @param accessmode: allow or deny mode
     * @param permission: what permission to be applied
     * @param datastore: dataStore
     * @throws OperationException 
     */
    private void applyDataStoreLevelDataPermission(final String entity, final String accessmode,
            final DataPermission permission, final String datastore) throws OperationException{
        List<String> collections = collectionManager.listTables(datastore);
        for(String collection : collections){
            applyCollectionLevelDataPermission(entity, accessmode, permission, datastore, collection);
        }
    }
    
    /**
     * apply a given data permission for a given user at a collection level
     * 
     * @param entity : user/group
     * @param accessmode: allow or deny mode
     * @param permission: what permission to be applied
     * @param datastore
     * @param collection
     * @throws OperationException 
     */
    private void applyCollectionLevelDataPermission(final String entity,final String accessmode, 
            final DataPermission permission, final String datastore, final String collection) throws OperationException{
        Schema schema = schemaManager.readSchema(datastore, collection);
        Set<String> columns = schema.getColumnMap().keySet();
        columns.stream().forEach((column) -> {
            setDataPermission(entity, accessmode, permission, datastore+"."+collection+"."+column);
         });
    }
    
    /**
     * This will set the permission for a given data level for a given entity based on given access mode
     * 
     * @param entity : user/group
     * @param accessmode: allow or deny mode
     * @param permission: what permission to be applied
     * @param dbLevel : data hierarchy level
     */
    private void setDataPermission(final String entity, final String accessmode, final DataPermission permission, final String dbLevel){
        
        DataPermission finalPermission = permission;
        // if deny is selected, then use the REMOVE PERMISSIONs accordingly
        if("deny".equals(accessmode)){
            switch(finalPermission){
                case ALL:
                    finalPermission = DataPermission.REMOVE_ALL;
                    break;
                case READ:
                    finalPermission = DataPermission.REMOVE_READ;
                    break;
                case WRITE:
                    finalPermission = DataPermission.REMOVE_WRITE;
                    break;
            }
        };
        
        // get the 
        Map<String, Integer> dataLevelMap = someMap.get(entity);
        // no entry for this user 
        if( dataLevelMap == null) dataLevelMap = new HashMap<>();
        // no previous record for this data level
        Integer oldPermission = dataLevelMap.get(dbLevel)==null ? DataPermission.NONE.getValue(): dataLevelMap.get(dbLevel);
        Integer news = DataPermission.NONE.getValue();
        switch(accessmode){
            case "allow":
                news = oldPermission | permission.getValue();
                break;
            case "deny":
                news = oldPermission & permission.getValue();
                break;
        }
        dataLevelMap.put(dbLevel, news);
        someMap.put(entity, dataLevelMap);
    }
    
    private List<JSONObject> convertToJson(){
        List<JSONObject> userPermissions = new ArrayList<>();
        for(String entity: someMap.keySet()){
            JSONObject pJson = new JSONObject(someMap.get(entity));
            pJson.put("entity", entity);
            userPermissions.add(pJson);
        }
        return userPermissions;
    }    

}
    
