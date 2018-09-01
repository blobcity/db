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
//import org.apache.hadoop.io.Text;

/**
 * This file stores the configuration for running Hadoop MapReduce jobs inside database.
 * As of this moment, some of the configs are hard coded but later on, those can be a user preference.
 * 
 * @author sanketsarang
 */
@Deprecated //for map-reduce jar failing docker security check
public class HadoopConfig {
//
//    /* Output types of Mapper class.  This will be a user preference later on */
//    public static Class mapOutputKeyClass = Text.class;
//    public static Class mapOutputValueClass = Text.class;
//
//    /* Output types of Reducer class. This will be a user preference later on */
//    public static Class outputKeyClass = Text.class;
//    public static Class outputValueClass = Text.class;
//
//    /* Location of config files required by Hadoop MapReduce. By default, these files have been added to the database jar itself */
//    public static String configLocation = "";
//
//    /* Config files required by Hadoop Map Reducer which are included in database only */
//    public static String coreSiteFileName = "core-site.xml";
//    public static String mapRedFileName = "mapred-site.xml";
//
//
//    /* A temporary folder required by Hadoop Map Reduce. This is integrated in mapred-site.xml now*/
//    public static String outputFolder = BSql.SEPERATOR + "tmp" + BSql.SEPERATOR + "hadoop-jobs" + BSql.SEPERATOR;
    
}
