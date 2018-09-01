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

package com.blobcity.db.olap;

import com.blobcity.db.exceptions.OperationException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * This is a storage class for data cubes. 
 * Each entry in the hashMap corresponds to a dataCube name and a dataCube instance.
 *
 * @author sanketsarang
 */
@Component
public class DataCubeStore {

    private static DataCubeStore instance;
    // datacube name mapped to datacube in memory
    private static HashMap<String, DataCube> store;
    
    @PostConstruct
    private void init() {
        instance = this;
        store = new HashMap<>();
    }

    /**
     *
     * @return
     */
    public static final DataCubeStore getInstance() {
        return instance;
    }

    //TODO: Create methods here

    /**
     *
     * @param cubeName
     * @return
     */
        public static boolean exists(String cubeName) {
        return store.containsKey(cubeName);
    }

    /**
     *
     * @param cube
     */
    public static void add(DataCube cube) {
        store.put(cube.getName(), cube);
    }

    /**
     *
     * @param name
     * @param tableName
     * @return
     */
    public static DataCube addCube(String name, String tableName) {
        DataCube cube = new DataCube();
        cube.setName(name);
        cube.setTableName(tableName);
        store.put(name, cube);
        return cube;
    }

    /**
     *
     * @param cube
     */
    public static void delete(DataCube cube) {
        store.remove(cube.getName());
    }

    /**
     *
     * @param name
     * @return
     * @throws OperationException
     */
    public static boolean delete(String name) throws OperationException {
        if (store.containsKey(name)) {
            DataCube cube = store.get(name);
            cube.clearAll();
            store.remove(name);
            return true;
        }
        return false;
    }

    /**
     *
     * @param name
     * @return
     */
    public static DataCube getCube(String name) {
        return store.get(name);
    }

    /**
     *
     * @param cube
     */
    public static void setCube(DataCube cube) {
        store.put(cube.getName(), cube);
    }

    /**
     *
     * @param oldName
     * @param newName
     */
    public static void rename(String oldName, String newName) {
        DataCube cube = store.get(oldName);
        if (cube != null) {
            store.remove(oldName, store.get(oldName));
            store.put(newName, cube);
        }
    }
    
    /**
     *
     * @return
     */
    public static Set<String> listDataCubes(){
        return store.keySet();
    }
}
