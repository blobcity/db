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

package com.blobcity.db.sql.statements;

import com.foundationdb.sql.types.DataTypeDescriptor;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks if the collation is specified in the column definition node. This is a temporary class until Collation is implemented
 *
 * @author akshaydewan
 */
public class CollationChecker {

    private static final Logger logger = LoggerFactory.getLogger(CollationChecker.class.getName());

    public static void check(final DataTypeDescriptor dataTypeDescriptor, final List<String> warnings) {
        String collation = dataTypeDescriptor.getCharacterAttributes() == null ? "" : dataTypeDescriptor.getCharacterAttributes().getCollation();
        if (!collation.isEmpty()) {
            String msg = "User specified collation " + collation + " will be ignored";
            logger.info(msg);
            warnings.add(msg);
        }
    }

}
