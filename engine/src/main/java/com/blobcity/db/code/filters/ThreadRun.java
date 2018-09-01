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

package com.blobcity.db.code.filters;

import com.blobcity.db.exceptions.OperationException;
import java.util.concurrent.Callable;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class ThreadRun implements Callable{
    private FilterParallelExecutor executor;
    private JSONObject row;
    private String app;
    private String filter;
    private int seq;
    
    public ThreadRun(FilterParallelExecutor executor, String app, String filter, JSONObject row, int seq){
        this.app = app;
        this.filter = filter;
        this.row = row;
        this.seq = seq;
        this.executor = executor;
    }

    public ThreadRun() {
    }
    
    @Override
    public Object call() throws OperationException{
        Object abc1 = executor.executeCheckMethod(app, filter, row, seq) ;
        return abc1;
    }
    
}
