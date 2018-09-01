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

package com.blobcity.lib.functions.spam;

/**
 * @author sanketsarang
 */
public interface SpamDetector {

    /**
     * Returns if the message passed is possibly spam or not. Accuracy is based on the current training accuracy of
     * the spam detection model.
     * @param message the message to be checked for spam
     * @param title optional parameter, represents title section of the primary message to be checked for spam
     * @return <code>true</code> if the message is possibly spam; <code>false</code> otherwise
     */
    public boolean isSpam(final String message, final String title);
}
