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

package com.blobcity.db.util;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author akshaydewan
 */
public class FileNameEncodingTest {

    public FileNameEncodingTest() {
    }

    static final List<Character> allowedChars;

    static {
        allowedChars = new ArrayList<>();
        for (char i = 'a'; i <= 'z'; i++) {
            allowedChars.add(i);
        }
        for (char i = 'A'; i <= 'Z'; i++) {
            allowedChars.add(i);
        }

        for (char i = '0'; i <= '9'; i++) {
            allowedChars.add(i);
        }
        allowedChars.add('.');
        allowedChars.add('-');
        allowedChars.add('_');
        allowedChars.add('%');
    }

    @BeforeClass

    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    private boolean isAllowed(final String output) {
        if (output.length() == 1 && output.equals(".")) {
            return false;
        }
        if (output.length() == 2 && output.equals("..")) {
            return false;
        }
        for (Character c : output.toCharArray()) {
            if (!allowedChars.contains(c)) {
                return false;
            }
        }
        return true;
    }

    private void testEncode(String input) throws OperationException {
        String output = FileNameEncoding.encode(input);
        if (!isAllowed(output)) {
            fail("output contains characters which are not allowed: " + output);
        }
        System.out.println(input + " => " + output);
        String decoded = FileNameEncoding.decode(output);
        assertEquals(decoded, input);
    }

    @Test
    public void encodeTest() throws OperationException {
        String input = ".test/file^!?name<foo>bar*what?";
        testEncode(input);
    }

    @Test
    public void testOtherLanguage() throws OperationException {
        //devnagiri
        String input = "देवनागरी लिपि";
        testEncode(input);
        //Kanji
        input = "当用漢字";
        testEncode(input);
        //Symbols
        input = "©˚¬©∫xdx";
        testEncode(input);
    }

    @Test
    public void testOSReserved() throws OperationException {
        testEncode(".");
        testEncode("..");
    }

    @Test
    public void testLengthExceed() {
        String input = "f%this is*^!@#? is_ƒåœ®∫ƒ®hellowå∫∫23/123/e/$@!$˚∆˙ˆ˙¥∫ç˜åasdad";
        try {
            FileNameEncoding.encode(input);
        } catch (OperationException ex) {
            LoggerFactory.getLogger(FileNameEncodingTest.class.getName()).info("OperationException was caught");
            assertEquals(ex.getErrorCode(), ErrorCode.STRING_LENGTH_EXCEEDED);
        }
    }

    @Test
    public void omgSuperCrazyTest() throws OperationException {
        String[] inputs = new String[]{"key1,value1", "विकिपीडिया:इण्टरनेट पर", "विकिपीडिया,value3", "इण्टरनेट,साधन", "key5,文字化け€àáßèëœ", "文字化け€àáßèëœ, value6", "€àáßèëœ,文字化け€àáßèëœ", "ひらがな, ひらがな", "カタカナ,カタカナ", "感じ,感じ", "ぶびばぱぴ,ぶびばぱぴ", "は゛ひ゛ふ゛は゜ひ゜ーヾ,は゛ひ゛ふ゛は゜ひ゜ーヾ", "งานออกช้เครื่องระบบ สากล", "शुक्जनवरी ०७२००५,वरी ०७ २००५", "قال عالم إيطالي يعمل الأحمر.", "자동으로 합니다", "Khoa học gia 30 năm", "àéîōũ,àéîōũ", "você nós mãe avô irm", "€ŒœŠš™©‰ƒ,€ŒœŠš™©‰ƒ", "がざばがざばだぱ", "�����	,�", "c,abcאבגדabc", "สวัสดี,สวัสดี", "حْمـَنِ الرَّحِيمِ	,بِسْمِ الل", "عد١٩٩٨	,عدد م١٩٩٨	"};
        for (String input : inputs) {
            testEncode(input);
        }
    }

    @Test
    public void testNormal() throws OperationException {
        testEncode("PhillipsSHP1900");
    }

}
