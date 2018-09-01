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

package com.blobcity.lib.database.bean.manager.interfaces;

/**
 * Interface to mark beans which can be stopped. Used to target self startup beans (such as the end point beans) that need to be shut down for the application
 * to end.
 *
 * @author javatarz (Karun Japhet)
 */
public interface Stoppable {

    /**
     * Causes the entity to trigger a shutdown.
     */
    public void stop();
}
