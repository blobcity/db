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

import com.blobcity.db.code.ManifestLang;
import com.blobcity.db.code.ManifestParserBean;
import com.blobcity.db.exceptions.OperationException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author sanketsarang
 */
@Ignore
public class ManifestParserTest extends ManifestParserBean {

    private List<String> lines;

    public ManifestParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {

    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        lines = new ArrayList<>();
        lines.add("MANIFEST-VERSION: 1.0");
        lines.add("PROCEDURES");
        lines.add("com.blobcity.test.procedures.Procedure1");
        lines.add("com.blobcity.test.procedures.Procedure2");
        lines.add("com.blobcity.test.procedures.Procedure3");
        lines.add("TRIGGERS");
        lines.add("com.blobcity.test.triggers.Trigger1");
        lines.add("com.blobcity.test.triggers.Trigger2");
        lines.add("com.blobcity.test.triggers.Trigger3");
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testProcedureParsing() {
        System.out.println("Test Procedures: Begin");
        try {
            List<String> list = getSection(lines, ManifestLang.PROCEDURES);
            assertEquals(3, list.size());
            assertEquals("com.blobcity.test.procedures.Procedure1", list.get(0));
            assertEquals("com.blobcity.test.procedures.Procedure2", list.get(1));
            assertEquals("com.blobcity.test.procedures.Procedure3", list.get(2));
            System.out.println("Test Procedures: Successful");
        } catch (OperationException ex) {
            LoggerFactory.getLogger(ManifestParserTest.class.getName()).error(null, ex);
            fail("Test Procedures: Failed");
        }
        System.out.println("Test Procedures: End");
    }

    @Test
    public void testTriggerParsing() {
        System.out.println("Test Triggers: Begin");
        try {
            List<String> list = getSection(lines, ManifestLang.TRIGGERS);
            assertEquals(3, list.size());
            assertEquals("com.blobcity.test.triggers.Trigger1", list.get(0));
            assertEquals("com.blobcity.test.triggers.Trigger2", list.get(1));
            assertEquals("com.blobcity.test.triggers.Trigger3", list.get(2));
            System.out.println("Test Triggers: Successful");
        } catch (OperationException ex) {
            LoggerFactory.getLogger(ManifestParserTest.class.getName()).error(null, ex);
            fail("Test Triggers: Failed");
        }
        System.out.println("Test Triggers: End");
    }
}
