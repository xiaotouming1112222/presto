/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestWindowQueries;
import com.facebook.presto.tests.H2QueryRunner;
import org.testng.annotations.Test;

public class TestWindowQueries
        extends AbstractTestWindowQueries
{
    private String expErrMsg1 = "\\*frameType == \\*orderByType Window frame of type RANGE does not match types of the ORDER BY and frame column";
    private String expErrMsg2 = "Unsupported window type: 2";
    private String expErrMsg3 = "Scalar function name not registered: presto.default.fail, called with arguments:";
    private String expErrMsg4 = "\\*frameType == \\*orderByType Window frame of type RANGE does not match types of the ORDER BY and frame column|Scalar function name not registered: presto.default.fail, called with arguments:";
    private String expErrMsg5 = "Scalar function presto.default.plus not registered with arguments:";
    private String expErrMsg6 = "Scalar function presto.default.gte not registered with arguments";

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(false);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return new H2QueryRunner();
    }

    @Override
    protected void createTables()
    {
        QueryRunner javaQueryRunner = null;
        try {
            javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(true);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        NativeQueryRunnerUtils.createAllTables(javaQueryRunner);
        javaQueryRunner.close();
    }

    @Override
    // TODO_PRESTISSIMO_FIX - Currently disabled because the query results in this testcase are inconsistent.
    @Test(enabled = false)
    public void testAllPartitionSameValues()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 1, 1, 1) T(a)",
                        expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 1, 1, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES 1, 1, 1) T(a)",
                expErrMsg1, true);
    }

    @Test
    public void testAllPartitionSameValuesGroup()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 'a', 'a', 'a') T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 'a', 'a', 'a') T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES 'a', 'a', 'a') T(a)",
                expErrMsg2, true);

        // test frame bounds at partition bounds
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 10 PRECEDING AND 10 FOLLOWING) " +
                        "FROM (VALUES 'a', 'a', 'a') T(a)",
                expErrMsg2, true);
    }

    @Override
    @Test
    public void testConstantOffset()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS CURRENT ROW) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 0 PRECEDING AND 0 FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                expErrMsg2, true);
    }

    @Override
    @Test(enabled = false)
    public void testEmptyFrame()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 10 PRECEDING) " +
                        "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 10 FOLLOWING AND 1 FOLLOWING) " +
                        "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 0.5 FOLLOWING AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, 2, 4) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 1.0, 1.1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a NULLS LAST RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 1.0, 1.1, null) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 1.0, 1.1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES null, 1.0, 1.1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 1, 2) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES null, 1, 2) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1.5 PRECEDING) " +
                        "FROM (VALUES null, 1, 2) T(a)",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testEmptyFrameGroup()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 90 PRECEDING AND 100 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 100 FOLLOWING AND 90 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);
    }

    @Override
    public void testInvalidOffset()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC RANGE x PRECEDING) " +
                        "FROM (VALUES (1, 0.1), (2, -0.2)) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (2, -0.2)) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE x PRECEDING) " +
                        "FROM (VALUES (1, 0.1), (2, -0.2)) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (2, -0.2)) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE x PRECEDING) " +
                        "FROM (VALUES (1, 0.1), (2, null)) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (2, null)) T(a, x)",
                expErrMsg4, true);

        // fail if offset is invalid for null sort key
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (null, null)) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (null, -0.1)) T(a, x)",
                expErrMsg4, true);

        // test invalid offset of different types
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, BIGINT '-1')) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, INTEGER '-1')) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (SMALLINT '1', SMALLINT '-1')) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (TINYINT '1', TINYINT '-1')) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, -1.1e0)) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, REAL '-1.1')) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, -1.0001)) T(a, x)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' YEAR)) T(a, x)",
                expErrMsg6, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' MONTH)) T(a, x)",
                expErrMsg6, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' DAY)) T(a, x)",
                expErrMsg6, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' HOUR)) T(a, x)",
                expErrMsg6, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' MINUTE)) T(a, x)",
                expErrMsg6, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' SECOND)) T(a, x)",
                expErrMsg6, true);
    }

    @Override
    @Test
    public void testInvalidOffsetGroup()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                expErrMsg2, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, 1), (2, null)) T(a, x)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (2, null)) T(a, x)",
                expErrMsg2, true);

        // fail if offset is invalid for null sort key
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (null, null)) T(a, x)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (null, -1)) T(a, x)",
                expErrMsg2, true);

        // test invalid offset of different types
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, BIGINT '-1')) T(a, x)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, INTEGER '-1')) T(a, x)",
                expErrMsg2, true);
    }

    @Override
    @Test
    public void testMixedTypeFrameBounds()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);
    }

    @Override
    @Test
    public void testMixedTypeFrameBoundsAscendingNullsFirst()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testMixedTypeFrameBoundsAscendingNullsLast()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 0.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testMixedTypeFrameBoundsDescendingNullsFirst()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testMixedTypeFrameBoundsDescendingNullsLast()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testMultipleWindowFunctions()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT x, array_agg(date) OVER(ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), avg(number) OVER(ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES " +
                        "(2, DATE '2222-01-01', 4.4), " +
                        "(1, DATE '1111-01-01', 2.2), " +
                        "(3, DATE '3333-01-01', 6.6)) T(x, date, number)",
                expErrMsg3, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT x, array_agg(a) OVER(ORDER BY x RANGE BETWEEN 2 PRECEDING AND CURRENT ROW), array_agg(a) OVER(ORDER BY x RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) " +
                        "FROM (VALUES " +
                        "(1.0, 1), " +
                        "(2.0, 2), " +
                        "(3.0, 3), " +
                        "(4.0, 4), " +
                        "(5.0, 5), " +
                        "(6.0, 6)) T(x, a)",
                expErrMsg3, true);
    }

    @Override
    @Test
    public void testMultipleWindowFunctionsGroup()
    {
        // two functions with frame type GROUPS
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT x, array_agg(date) OVER(ORDER BY x GROUPS BETWEEN 1 PRECEDING AND 1 PRECEDING), avg(number) OVER(ORDER BY x GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) " +
                        "FROM (VALUES " +
                        "(2, DATE '2222-01-01', 4.4), " +
                        "(1, DATE '1111-01-01', 2.2), " +
                        "(3, DATE '3333-01-01', 6.6)) T(x, date, number)",
                expErrMsg2, true);

        // three functions with different frame types
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT " +
                        "x, " +
                        "array_agg(a) OVER(ORDER BY x RANGE BETWEEN 2 PRECEDING AND CURRENT ROW), " +
                        "array_agg(a) OVER(ORDER BY x GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING), " +
                        "array_agg(a) OVER(ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES " +
                        "(1.0, 1), " +
                        "(2.0, 2), " +
                        "(3.0, 3), " +
                        "(4.0, 4), " +
                        "(5.0, 5), " +
                        "(6.0, 6)) T(x, a)",
                expErrMsg2, true);
    }

    @Override
    public void testNonConstantOffset()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN x * 10 PRECEDING AND y / 10.0 FOLLOWING) " +
                        "FROM (VALUES (1, 0.1, 10), (2, 0.2, 20), (4, 0.4, 40)) T(a, x, y)",
                expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN x * 10 PRECEDING AND y / 10.0 FOLLOWING) " +
                        "FROM (VALUES (1, 0.1, 10), (2, 0.2, 20), (4, 0.4, 40), (null, 0.5, 50)) T(a, x, y)",
                expErrMsg4, true);
    }

    @Override
    @Test
    public void testNonConstantOffsetGroup()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x PRECEDING AND y FOLLOWING) " +
                        "FROM (VALUES ('a', 1, 1), ('b', 2, 0), ('c', 0, 3)) T(a, x, y)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x FOLLOWING AND y FOLLOWING) " +
                        "FROM (VALUES ('a', 1, 1), ('b', 2, 0), ('c', 3, 3), ('d', 0, 0)) T(a, x, y)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x PRECEDING AND y PRECEDING) " +
                        "FROM (VALUES ('a', 1, 1), ('b', 0, 2), ('c', 2, 1), ('d', 0, 2)) T(a, x, y)",
                expErrMsg2, true);
    }

    @Override
    @Test
    public void testNoValueFrameBoundsGroup()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                expErrMsg2, true);
    }

    @Override
    public void testNullsSortKey()
    {
        // TODO_PRESTISSIMO_FIX - Currently disabled because the query results are not consistent
//        assertQuery("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
//                        "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) T(a)",
//                "VALUES " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[1, 1, 2, 2], " +
//                        "ARRAY[1, 1, 2, 2], " +
//                        "ARRAY[1, 1, 2, 2, 3], " +
//                        "ARRAY[1, 1, 2, 2, 3], " +
//                        "ARRAY[2, 2, 3]");

        // TODO_PRESTISSIMO_FIX - Currently disabled because the query results are not consistent
//        assertQuery("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
//                        "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) T(a)",
//                "VALUES " +
//                        "ARRAY[1, 1, 2, 2], " +
//                        "ARRAY[1, 1, 2, 2], " +
//                        "ARRAY[1, 1, 2, 2, 3], " +
//                        "ARRAY[1, 1, 2, 2, 3], " +
//                        "ARRAY[2, 2, 3], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null]");

        // TODO_PRESTISSIMO_FIX - Currently disabled because the query results are not consistent
//        assertQuery("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
//                        "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) T(a)",
//                "VALUES " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[3, 2, 2], " +
//                        "ARRAY[3, 2, 2, 1, 1], " +
//                        "ARRAY[3, 2, 2, 1, 1], " +
//                        "ARRAY[2, 2, 1, 1], " +
//                        "ARRAY[2, 2, 1, 1]");

        // TODO_PRESTISSIMO_FIX - Currently disabled because the query results are not consistent
//        assertQuery("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
//                        "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) T(a)",
//                "VALUES " +
//                        "ARRAY[3, 2, 2], " +
//                        "ARRAY[3, 2, 2, 1, 1], " +
//                        "ARRAY[3, 2, 2, 1, 1], " +
//                        "ARRAY[2, 2, 1, 1], " +
//                        "ARRAY[2, 2, 1, 1], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null], " +
//                        "ARRAY[null, null, null, null]");

        assertQuery("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2) T(a)",
                "VALUES " +
                        "ARRAY[1, 2, null, null], " +
                        "ARRAY[1, 2, null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null]");

        // TODO_PRESTISSIMO_FIX - Currently disabled because the query results are not consistent
//        assertQuery("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) " +
//                        "FROM (VALUES 1, null, null, 2) T(a)",
//                "VALUES " +
//                        "ARRAY[1, 2], " +
//                        "ARRAY[1, 2], " +
//                        "ARRAY[1, 2, null, null], " +
//                        "ARRAY[1, 2, null, null]");

        assertQuery("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2) T(a)",
                "VALUES " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null], " +
                        "ARRAY[null, null, 1, 2], " +
                        "ARRAY[null, null, 1, 2]");

        assertQuery("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2) T(a)",
                "VALUES " +
                        "ARRAY[null, null, 1, 2], " +
                        "ARRAY[null, null, 1, 2], " +
                        "ARRAY[1, 2], " +
                        "ARRAY[1, 2]");
    }

    @Override
    @Test
    public void testOnlyNullsGroup()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) " +
                        "FROM (VALUES CAST(null AS integer), null, null) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES CAST(null AS integer), null, null) T(a)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES CAST(null AS integer), null, null) T(a)",
                expErrMsg2, true);
    }

    @Override
    @Test
    public void testWindowPartitioning()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y')) T(a, p)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y'), (null, null), (null, null), (1, null)) T(a, p)",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testWindowPartitioningGroup()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y')) T(a, p)",
                expErrMsg2, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 0 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y'), (null, null), (null, null), (1, null)) T(a, p)",
                expErrMsg2, true);
    }

    @Override
    public void testTypes()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN DOUBLE '0.5' PRECEDING AND TINYINT '1' FOLLOWING) " +
                        "FROM (VALUES 1, null, 2) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 0.5 PRECEDING AND 1.000 FOLLOWING) " +
                        "FROM (VALUES REAL '1', null, 2) T(a)",
                expErrMsg1, true);

        // TODO_PRESTISSIMO_FIX - Currently disabled because the query results are not consistent
//        assertQuery("SELECT x, array_agg(x) OVER(ORDER BY x DESC RANGE BETWEEN interval '1' month PRECEDING AND interval '1' month FOLLOWING) " +
//                        "FROM (VALUES DATE '2001-01-31', DATE '2001-08-25', DATE '2001-09-25', DATE '2001-09-26') T(x)",
//                "VALUES " +
//                        "(DATE '2001-09-26', ARRAY[DATE '2001-09-26', DATE '2001-09-25']), " +
//                        "(DATE '2001-09-25', ARRAY[DATE '2001-09-26', DATE '2001-09-25', DATE '2001-08-25']), " +
//                        "(DATE '2001-08-25', ARRAY[DATE '2001-09-25', DATE '2001-08-25']), " +
//                        "(DATE '2001-01-31', ARRAY[DATE '2001-01-31'])");

        // January 31 + 1 month sets the frame bound to the last day of February. March 1 is out of range.
        // TODO_PRESTISSIMO_FIX - Currently disabled because the query results are not consistent
//        assertQuery("SELECT x, array_agg(x) OVER(ORDER BY x RANGE BETWEEN CURRENT ROW AND interval '1' month FOLLOWING) " +
//                        "FROM (VALUES DATE '2001-01-31', DATE '2001-02-28', DATE '2001-03-01') T(x)",
//                "VALUES " +
//                        "(DATE '2001-01-31', ARRAY[DATE '2001-01-31', DATE '2001-02-28']), " +
//                        "(DATE '2001-02-28', ARRAY[DATE '2001-02-28', DATE '2001-03-01']), " +
//                        "(DATE '2001-03-01', ARRAY[DATE '2001-03-01'])");

        // H2 and Presto has some type conversion problem for Interval type, hence use the same query runner for this query
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT x, array_agg(x) OVER(ORDER BY x RANGE BETWEEN interval '1' year PRECEDING AND interval '1' month FOLLOWING) " +
                        "FROM (VALUES " +
                        "INTERVAL '1' month, " +
                        "INTERVAL '2' month, " +
                        "INTERVAL '5' year) T(x)",
                expErrMsg5, true);
    }
}
