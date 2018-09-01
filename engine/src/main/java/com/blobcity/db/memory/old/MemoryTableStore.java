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

package com.blobcity.db.memory.old;

import com.blobcity.db.exceptions.OperationException;
import java.util.HashMap;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class MemoryTableStore {

    //TODO: Create the store here
    private static MemoryTableStore instance;
    // app.table mapped to memorytable
    private static HashMap<String, MemoryTable> store = new HashMap<>();
    private static Logger logger = LoggerFactory.getLogger(MemoryTableStore.class);

    @PostConstruct
    private void init() {
        instance = this;
    }

    /**
     *
     * @return
     */
    public static final MemoryTableStore getInstance() {
        return instance;
    }

    // This method keeps the unit test happy

    /**
     *
     */
        public static void createStore() {
        if(store == null) {
            store = new HashMap<>();
        }
    }
    
    //TODO: Create methods here

    /**
     *
     * @param tbl
     * @throws OperationException
     */
        public static void add(MemoryTable tbl) throws OperationException {
        if (!store.containsKey(tbl.getName())) {
            store.put(tbl.getName(), tbl);
        }
    }

    /**
     *
     * @param name
     */
    public static void add(String name) {
        MemoryTable tbl = new MemoryTable();
        tbl.setName(name);
        if (!store.containsKey(tbl.getName())) {
            store.put(tbl.getName(), tbl);
        }

    }

    /**
     *
     * @param tbl
     */
    public static void delete(MemoryTable tbl) {
        store.remove(tbl.getName());
    }

    /**
     *
     * @param name
     */
    public static synchronized void delete(String name) {
        store.remove(name);
    }

    /**
     *
     * @param name
     * @return
     */
    public static MemoryTable getTable(String name) {
        return store.get(name);
    }

    /**
     *
     * @param tbl
     */
    public static void setTable(MemoryTable tbl) {
        store.put(tbl.getName(), tbl);
    }

    /**
     *
     * @param name
     * @return
     * @throws OperationException
     */
    public static boolean exists(String name) throws OperationException {
        MemoryTable tbl = store.get(name);
        return (tbl != null);
    }

    /**
     *
     * @param oldName
     * @param newName
     * @throws OperationException
     */
    public static void rename(String oldName, String newName) throws OperationException {
        MemoryTable tbl = store.get(oldName);
        if (tbl != null) {
            store.remove(oldName);
            store.put(newName, tbl);
        }
    }
}
