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

package com.blobcity.db.processors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author sanketsarang
 */
public class ProcessorExecutorService {

    private static ProcessorExecutorService ourInstance = new ProcessorExecutorService();

    public static ProcessorExecutorService getInstance() {
        return ourInstance;
    }

    private final ExecutorService executorService;

    private ProcessorExecutorService() {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void submit(ProcessHandler processHandler) {
        executorService.submit(processHandler);
    }
}
