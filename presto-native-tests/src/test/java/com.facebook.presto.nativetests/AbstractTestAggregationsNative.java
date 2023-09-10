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

import com.facebook.presto.tests.AbstractTestAggregations;
import org.testng.annotations.Test;

import static java.lang.String.format;

public abstract class AbstractTestAggregationsNative
        extends AbstractTestAggregations
{
    private String expErrMsg4 = "Aggregate function not registered: presto.default.*digest_agg|Failed to parse type.*digest.*double";

    @Override
    public void testApproximateCountDistinct()
    {
        String expErrMsg1 = "Aggregate function signature is not supported: presto.default.approx_distinct.*";
        String expErrMsg2 = "Failed to parse type.*time";
        String expErrMsg3 = "Failed to parse type.*char";

        // test NULL
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(NULL)", expErrMsg1, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(NULL, 0.023)", expErrMsg1, true);

        // test date
        assertQuery("SELECT approx_distinct(orderdate) FROM orders", "SELECT 2376");
        assertQuery("SELECT approx_distinct(orderdate, 0.023) FROM orders", "SELECT 2376");

        // test timestamp
        // TODO_UNKNOWN
        //assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP)) FROM orders", "SELECT 2443");
        // assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP), 0.023) FROM orders", "SELECT 2443");

        // test timestamp with time zone
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP WITH TIME ZONE)) FROM orders",
                expErrMsg1, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP WITH TIME ZONE), 0.023) FROM orders",
                expErrMsg1, true);

        // test time
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME)) FROM orders", expErrMsg2, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME), 0.023) FROM orders", expErrMsg2, true);

        // test time with time zone
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME WITH TIME ZONE)) FROM orders", expErrMsg2, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME WITH TIME ZONE), 0.023) FROM orders", expErrMsg2, true);

        // test short decimal
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(18, 0))) FROM orders", "SELECT 990");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(18, 0)), 0.023) FROM orders", "SELECT 990");

        // test long decimal
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(25, 20))) FROM orders", "SELECT 1013");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(25, 20)), 0.023) FROM orders", "SELECT 1013");

        // test real
        assertQuery("SELECT approx_distinct(CAST(custkey AS REAL)) FROM orders", "SELECT 982");
        assertQuery("SELECT approx_distinct(CAST(custkey AS REAL), 0.023) FROM orders", "SELECT 982");

        // test bigint
        assertQuery("SELECT approx_distinct(custkey) FROM orders", "SELECT 990");
        assertQuery("SELECT approx_distinct(custkey, 0.023) FROM orders", "SELECT 990");

        // test integer
        assertQuery("SELECT approx_distinct(CAST(custkey AS INTEGER)) FROM orders", "SELECT 1028");
        assertQuery("SELECT approx_distinct(CAST(custkey AS INTEGER), 0.023) FROM orders", "SELECT 1028");

        // test smallint
        assertQuery("SELECT approx_distinct(CAST(custkey AS SMALLINT)) FROM orders", "SELECT 1023");
        assertQuery("SELECT approx_distinct(CAST(custkey AS SMALLINT), 0.023) FROM orders", "SELECT 1023");

        // test tinyint
        assertQuery("SELECT approx_distinct(CAST((custkey % 128) AS TINYINT)) FROM orders", "SELECT 128");
        assertQuery("SELECT approx_distinct(CAST((custkey % 128) AS TINYINT), 0.023) FROM orders", "SELECT 128");

        // test double
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE)) FROM orders", "SELECT 1014");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE), 0.023) FROM orders", "SELECT 1014");

        // test varchar
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR)) FROM orders", "SELECT 1036");
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR), 0.023) FROM orders", "SELECT 1036");

        // test char
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(CAST(CAST(custkey AS VARCHAR) AS CHAR(20))) FROM orders", expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT approx_distinct(CAST(CAST(custkey AS VARCHAR) AS CHAR(20)), 0.023) FROM orders", expErrMsg3, true);

        // test varbinary
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR))) FROM orders", "SELECT 1036");
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR)), 0.023) FROM orders", "SELECT 1036");
    }

    @Test(dataProvider = "getType", enabled = true) @Override
    public void testStatisticalDigest(String type)
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), expErrMsg4, true);
    }

    /**
     * Comprehensive correctness testing is done in the TestQuantileDigestAggregationFunction and TestTDigestAggregationFunction
     */
    @Test(dataProvider = "getType", enabled = true) @Override
    public void testStatisticalDigestGroupBy(String type)
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                expErrMsg4, true);
    }

    /**
     * Comprehensive correctness testing is done in the TestMergeQuantileDigestFunction and TestMergeTDigestFunction
     */
    @Test(dataProvider = "getType", enabled = true) @Override
    public void testStatisticalDigestMerge(String type)
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT value_at_quantile(merge(%s), 0.5E0) > 0 FROM (SELECT partkey, %s_agg(CAST(orderkey AS DOUBLE)) as %s FROM lineitem GROUP BY partkey)",
                        type,
                        type,
                        type),
                expErrMsg4, true);
    }

    /**
     * Comprehensive correctness testing is done in the TestMergeQuantileDigestFunction and TestMergeTDigestFunction
     */
    @Test(dataProvider = "getType", enabled = true) @Override
    public void testStatisticalDigestMergeGroupBy(String type)
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(format("SELECT partkey, value_at_quantile(merge(%s), 0.5E0) > 0 " +
                                "FROM (SELECT partkey, suppkey, %s_agg(CAST(orderkey AS DOUBLE)) as %s FROM lineitem GROUP BY partkey, suppkey)" +
                                "GROUP BY partkey",
                        type,
                        type,
                        type),
                expErrMsg4, true);
    }

    @Override
    @Test
    public void testSumDataSizeForStats()
    {
        // varchar
        assertQuery("SELECT \"sum_data_size_for_stats\"(comment) FROM orders", "SELECT 787364");

        // char
        // Presto removes trailing whitespaces when casting to CHAR.
        // Hard code the expected data size since there is no easy to way to compute it in H2.
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT \"sum_data_size_for_stats\"(CAST(comment AS CHAR(1000))) FROM orders",
                "Failed to parse type \\[char\\(1000\\)\\]", true);

        // varbinary
        assertQuery("SELECT \"sum_data_size_for_stats\"(CAST(comment AS VARBINARY)) FROM orders", "SELECT 787364");

        // array
        assertQuery("SELECT \"sum_data_size_for_stats\"(ARRAY[comment]) FROM orders", "SELECT 847364");
        assertQuery("SELECT \"sum_data_size_for_stats\"(ARRAY[comment, comment]) FROM orders", "SELECT 1634728");

        // map
        assertQuery("SELECT \"sum_data_size_for_stats\"(map(ARRAY[1], ARRAY[comment])) FROM orders", "SELECT 907364");
        assertQuery("SELECT \"sum_data_size_for_stats\"(map(ARRAY[1, 2], ARRAY[comment, comment])) FROM orders", "SELECT 1754728");

        // row
        assertQuery("SELECT \"sum_data_size_for_stats\"(ROW(comment)) FROM orders", "SELECT 847364");
        assertQuery("SELECT \"sum_data_size_for_stats\"(ROW(comment, comment)) FROM orders", "SELECT 1634728");
    }

    @Override
    @Test
    public void testMaxDataSizeForStats()
    {
        // varchar
        assertQuery("SELECT \"max_data_size_for_stats\"(comment) FROM orders", "select 82");

        // char
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT \"max_data_size_for_stats\"(CAST(comment AS CHAR(1000))) FROM orders",
                "Failed to parse type \\[char\\(1000\\)\\]", true);

        // varbinary
        assertQuery("SELECT \"max_data_size_for_stats\"(CAST(comment AS VARBINARY)) FROM orders", "select 82");

        // max_data_size_for_stats is not needed for array, map and row
    }

    @Override
    public void testGroupByExtract()
    {
        String expErrMsg1 = "Type of argument to extract must be DATE, TIME, TIMESTAMP, or INTERVAL \\(actual varchar\\)";
        // TODO_PRESTISSIMO_FIX - Env issue (Should succeed with PARQUET data format)
        assertQueryFails("SELECT EXTRACT(YEAR FROM orderdate), count(*) FROM orders GROUP BY EXTRACT(YEAR FROM orderdate)", expErrMsg1, true);
        // TODO_PRESTISSIMO_FIX - Env issue (Should succeed with PARQUET data format)
        assertQueryFails("SELECT EXTRACT(YEAR FROM orderdate), count(*) FROM orders GROUP BY 1", expErrMsg1, true);
        // TODO_PRESTISSIMO_FIX - Env issue (Should succeed with PARQUET data format)
        // argument in group by
        assertQueryFails("SELECT EXTRACT(YEAR FROM orderdate), count(*) FROM orders GROUP BY orderdate", expErrMsg1, true);
    }

    @Override
    public void testGroupingSetMixedExpressionAndColumn()
    {
        String expErrMsg1 = "Unexpected parameters \\(varchar\\) for function month. Expected: month";
        // TODO_PRESTISSIMO_FIX - Env issue (Should succeed with PARQUET data format)
        assertQueryFails("SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), ROLLUP(suppkey)", expErrMsg1, true);
    }

    @Override
    public void testGroupingSetMixedExpressionAndOrdinal()
    {
        String expErrMsg1 = "Unexpected parameters \\(varchar\\) for function month. Expected: month";
        // TODO_PRESTISSIMO_FIX - Env issue (Should succeed with PARQUET data format)
        assertQueryFails("SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY 2, ROLLUP(suppkey)", expErrMsg1, true);
    }
}
