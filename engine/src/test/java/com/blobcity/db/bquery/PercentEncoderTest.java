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

package com.blobcity.db.bquery;

import com.blobcity.db.util.PercentEncoder;
import java.io.UnsupportedEncodingException;
import java.util.BitSet;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author akshaydewan
 */
public class PercentEncoderTest {

    @Test
    public void test() throws UnsupportedEncodingException {
        String input = "विकिपीडिया:इण्टरनेट पर हिन्दी के साधन\", \"विकिपीडिया,value3\", \"इण्टरनेट,साधन\", \"key5,文字化け€àáßèëœ\", \"文字化け€àáßèëœ, value6\", \"€àáßèëœ,文字化け€àáßèëœ\", \"ひらがな, ひらがな\", \"カタカナ,カタカナ\", \"感じ,感じ\", \"ぶびばぱぴ,ぶびばぱぴ\", \"は゛ひ゛ふ゛は゜ひ゜ーヾ,は゛ひ゛ふ゛は゜ひ゜ーヾ\", \"งานออกแบบรายการใช้เครื่องระบบ สากล,งานออกแบบรายการใช้เครื่องระบบ สากล\", \"शुक्रवारजनवरी ०७२००५ :,शुक्रवार जन वरी ०७ २००५ : \", \"قال عالم إيطالي يعمل في مشروع مسبار المريخ الفضائي إن الغازات التي تم اكتشاف وجودها على سطح المريخ قد تعطي دلائل على إمكانية وجود حياة على هذا الكوكب الأحمر.,قال عالم إيطالي يعمل في مشروع مسبار المريخ الفضائي إن الغازات التي تم اكتشاف وجودها على سطح المريخ قد تعطي دلائل على إمكانية وجود حياة على هذا الكوكب الأحمر.\", \"자동으로 로그인합니다,자동으로 로그인합니다\", \"Khoa học gia nổi danh của Đức từ chức vì giả mạo suốt 30 năm,Khoa học gia nổi danh của Đức từ chức vì giả mạo suốt 30 năm\", \"àéîōũ,àéîōũ\", \"você nós mãe avô irmã criança,você nós mãe avô irmã criança\", \"€ŒœŠš™©‰ƒ,€ŒœŠš™©‰ƒ\", \"がざばだぱか゛さ゛た゛は゜,がざばだぱか゛さ゛た゛は゜\", \"������������	,������������	\", \"abcאבגדabc,abcאבגדabc\", \"สวัสดี,สวัสดี\", \"بِسْمِ اللّهِ الرَّحْمـَنِ الرَّحِيمِ	,بِسْمِ اللّهِ الرَّحْمـَنِ الرَّحِيمِ	\", \"عدد مارس ١٩٩٨	,عدد مارس ١٩٩٨";
        byte[] encoded = PercentEncoder.encode(input.getBytes("UTF-8"), new BitSet());
        String decoded = PercentEncoder.decode(new String(encoded, "UTF-8"), "UTF-8");
        assertTrue(decoded.equals(input));
    }

}
