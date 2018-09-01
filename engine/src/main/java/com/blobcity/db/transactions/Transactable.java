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

package com.blobcity.db.transactions;

/**
 * Represents a generic transaction interface. All transaction capable operations must implement this interface and
 * support execution, commit and rollback operations.
 *
 * @author sanketsarang
 */
public interface Transactable {

    /**
     * Performs soft execution of the transaction, and returns ture if the execution is successful
     *
     * @return
     */
    public boolean softExecute();

    /**
     * Commits the transaction
     */
    public boolean commit();

    /**
     * Rolls back the transaction. This operation should not require performing any actual roll-back as the 
     * <code>execute()</code> operation is a soft execution
     */
    public boolean rollback();
}
