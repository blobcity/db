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

/**
 * Enum for various states of a Map Reduce Job
 *
 * @author sanketsarang
 */
public enum MRJobStatus {
    CANCELLED("cancelled"),
    COMPLETED("completed"),
    ERROR("error"),
    QUEUED("queued"),
    RUNNING("running"),
    //when history is loaded from the file
    UNKNOWN("unknown");
    
    private String status;
    
    MRJobStatus(final String status){
        this.status = status;
    }
    
    public static MRJobStatus fromString(String status){
        for(MRJobStatus jobStatus: MRJobStatus.values()){
            if(status.equals(jobStatus.getStatus())) return jobStatus;
        }
        return MRJobStatus.UNKNOWN;
    }
    
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    
}
