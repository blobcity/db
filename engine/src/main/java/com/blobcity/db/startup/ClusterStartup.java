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

package com.blobcity.db.startup;

import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.config.ConfigProperties;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Attempts connecting to all nodes that are listed in the cluster table of this node.
 *
 * @author sanketsarang
 */
@Component
public class ClusterStartup {
    
    private static final Logger logger = LoggerFactory.getLogger(StorageStartup.class);
    
    @Autowired
    private ConfigBean configBean;
    
    public void startup() {
        logger.info("Starting cluster service");
        
        JSONArray jsonArray = (JSONArray)configBean.getProperty(ConfigProperties.CLUSTER_NODES);
        if(jsonArray == null || jsonArray.length() == 0) {
            logger.info("This node is not part of a cluster. Service started in standalone mode.");
            return;
        }
        
        logger.info("Connecting to nodes " + jsonArray.toString());
        
        //TODO: Start connection service for connecting to the missing cluster nodes
    }
}
