package com.facebook.presto.nativetests;

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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueries;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_ENABLED;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_PERCENTAGE;
import static com.facebook.presto.SystemSessionProperties.LEGACY_UNNEST;
import static com.facebook.presto.SystemSessionProperties.MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS;
import static com.facebook.presto.SystemSessionProperties.REMOVE_MAP_CAST;
import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_PLAN_WITH_EMPTY_INPUT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestQueriesNative
        extends AbstractTestQueries
{
    private String expErrMsg1 = "inferredType Failed to parse type \\[P4HyperLogLog\\]. Type not registered";
    private String expErrMsg2 = "Scalar function name not registered: presto.default.apply";
    private String expErrMsg3 = "Scalar function name not registered: presto.default.fail";
    private String expErrMsg4 = "inferredType Failed to parse type \\[KHyperLogLog\\]. Type not registered";
    private String expErrMsg5 = "Failed to parse type \\[char\\(.*\\)\\]. syntax error, unexpected LPAREN, expecting WORD";
    private String expErrMsg6 = "Scalar function name not registered: presto.default.at_timezone, called with arguments: \\(TIMESTAMP WITH TIME ZONE, VARCHAR\\)";
    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";
    protected static final DateTimeFormatter ZONED_DATE_TIME_FORMAT = DateTimeFormatter.ofPattern(SqlTimestampWithTimeZone.JSON_FORMAT);
    private ZonedDateTime zonedDateTime(String value)
    {
        return ZONED_DATE_TIME_FORMAT.parse(value, ZonedDateTime::from);
    }

    @Override
    @Test
    public void testCustomAdd()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT custom_add(orderkey, custkey) FROM orders",
                " Scalar function name not registered: presto.default.custom_add, called with arguments: \\(BIGINT, BIGINT\\).");
    }

    @Override
    @Test
    public void testCustomSum()
    {
        // TODO_PRESTISSIMO_FIX
        @Language("SQL") String sql = "SELECT orderstatus, custom_sum(orderkey) FROM orders GROUP BY orderstatus";
        assertQueryFails(sql, " Aggregate function not registered: presto.default.custom_sum");
    }

    @Override
    @Test
    public void testCustomRank()
    {
        @Language("SQL") String sql = "" +
                "SELECT orderstatus, clerk, sales\n" +
                ", custom_rank() OVER (PARTITION BY orderstatus ORDER BY sales DESC) rnk\n" +
                "FROM (\n" +
                "  SELECT orderstatus, clerk, sum(totalprice) sales\n" +
                "  FROM orders\n" +
                "  GROUP BY orderstatus, clerk\n" +
                ")\n" +
                "ORDER BY orderstatus, clerk";

        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, " Window function not registered: presto.default.custom_rank");
    }

    @Override
    @Test
    public void testAggregationOverUnknown()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT clerk, min(totalprice), max(totalprice), min(nullvalue), max(nullvalue) " +
                "FROM (SELECT clerk, totalprice, null AS nullvalue FROM orders) " +
                "GROUP BY clerk", "Aggregate function signature is not supported: presto.default.max\\(UNKNOWN\\).", true);
    }

    @Override
    @Test
    public void testApproxMostFrequentWithLong()
    {
        MaterializedResult actual1 = computeActual("SELECT approx_most_frequent(3, cast(x as bigint), 15) FROM (values 1, 2, 1, 3, 1, 2, 3, 4, 5) t(x)");
        assertEquals(actual1.getRowCount(), 1);
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of(1L, 3L, 2L, 2L, 3L, 2L));

        MaterializedResult actual2 = computeActual("SELECT approx_most_frequent(2, cast(x as bigint), 15) FROM (values 1, 2, 1, 3, 1, 2, 3, 4, 5) t(x)");
        assertEquals(actual2.getRowCount(), 1);
        assertEquals(actual2.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of(1L, 3L, 3L, 2L));
    }

    @Override
    @Test
    public void testGroupByLimit()
    {
        Session prefilter = Session.builder(getSession())
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT, "true")
                .build();
        MaterializedResult result1 = computeActual(prefilter, "select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        MaterializedResult result2 = computeActual("select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        assertEqualsIgnoreOrder(result1, result2, "Prefilter and without prefilter don't give matching results");

        // TODO_PRESTISSIMO_FIX
        assertQueryFails(prefilter, "select count(custkey), orderkey from orders where orderstatus='F' and orderkey < 50 group by orderkey limit 100", "Scalar function name not registered: presto.default.\\$operator\\$hash_code, called with arguments: \\(BIGINT\\)", true);
        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders where orderstatus='F' and orderkey < 50 group by orderkey limit 4)", "select 4");
        assertQuery(prefilter, "select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders where orderkey < 50) group by orderstatus limit 100", "values (5, 'F'), (10, 'O')");

        assertQuery(prefilter, "select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders where orderkey < 50) group by orderstatus having count(1) > 1 limit 100", "values (5, 'F'), (10, 'O')");

        prefilter = Session.builder(getSession())
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT, "true")
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS, "1")
                .build();

        result1 = computeActual(prefilter, "select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        result2 = computeActual("select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        assertEqualsIgnoreOrder(result1, result2, "Prefilter and without prefilter don't give matching results");

        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders group by orderkey limit 100000)", "values 15000");
        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders group by orderkey limit 4)", "select 4");
        assertQuery(prefilter, "select count(1) from (select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders) group by orderstatus limit 100000)", "values 3");
    }

    @Override
    public void testUnknownMaxBy()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select max_by(x, y) from (select 1 x, null y)", "Aggregate function signature is not supported: presto.default.max_by\\(INTEGER, UNKNOWN\\)", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select max_by(x, y) from (select null x, 1 y)", "Unknown input types for presto.default.max_by.*aggregation", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select max_by(x, y) from (select null x, null y)", "Aggregate function signature is not supported: presto.default.max_by\\(UNKNOWN, UNKNOWN\\)", true);
    }

    @Override
    @Test
    public void testTryLambdaRepeated()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT x + x FROM (SELECT apply(a, i -> i * i) x FROM (VALUES 3) t(a))", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(a, i -> i * i) + apply(a, i -> i * i) FROM (VALUES 3) t(a)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(a, i -> i * i), apply(a, i -> i * i) FROM (VALUES 3) t(a)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        assertQuery("SELECT try(10 / a) + try(10 / a) FROM (VALUES 5) t(a)", "SELECT 4");
        assertQuery("SELECT try(10 / a), try(10 / a) FROM (VALUES 5) t(a)", "SELECT 2, 2");
    }

    @Override
    @Test
    public void testLambdaCapture()
    {
        // Test for lambda expression without capture can be found in TestLambdaExpression

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(0, x -> x + c1) FROM (VALUES 1) t(c1)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(0, x -> x + t.c1) FROM (VALUES 1) t(c1)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(c1 + 10, x -> apply(x + 100, y -> t.c1)) FROM (VALUES 1) t(c1)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x) FROM (VALUES 1) u(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x) FROM (VALUES 1) r(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 1) u(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 1) r(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 'a') r(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), z -> apply(3, y -> y + r.x)) FROM (VALUES 1) r(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);

        // TODO_PRESTISSIMO_FIX
        // reference lambda variable of the not-immediately-enclosing lambda
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);

        // in join post-filter
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT * FROM (VALUES true) t(x) left JOIN (VALUES 1001) t2(y) ON (apply(false, z -> apply(false, y -> x)))", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
    }

    @Override
    @Test
    public void testLambdaInAggregationContext()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(sum(x), i -> i * i) FROM (VALUES 1, 2, 3, 4, 5) t(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) GROUP BY x", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT x, apply(sum(y), i -> i * 10) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) GROUP BY x", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(8, x -> x + 1) FROM (VALUES (1, 2)) t(x,y) GROUP BY y", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> x.someField) FROM (VALUES (1,2)) t(x,y) GROUP BY y", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(sum(x), x -> x * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        // nested lambda expression uses the same variable name
        assertQueryFails("SELECT apply(sum(x), x -> apply(x, x -> x * x)) FROM (VALUES 1, 2, 3, 4, 5) t(x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
    }

    @Override
    @Test
    public void testLambdaInSubqueryContext()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(x, i -> i * i) FROM (SELECT 10 x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply((SELECT 10), i -> i * i)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);

        // TODO_PRESTISSIMO_FIX
        // with capture
        assertQueryFails("SELECT apply(x, i -> i * x) FROM (SELECT 10 x)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(x, y -> y * x) FROM (SELECT 10 x, 3 y)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(x, z -> y * x) FROM (SELECT 10 x, 3 y)", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
    }

    @Override
    @Test
    public void testNonDeterministic()
    {
        MaterializedResult materializedResult = computeActual("SELECT rand() FROM orders LIMIT 10");
        long distinctCount = materializedResult.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .distinct()
                .count();
        assertTrue(distinctCount >= 8, "rand() must produce different rows");

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(1, x -> x + rand()) FROM orders LIMIT 10", "Scalar function name not registered: presto.default.apply, called with arguments.*", true);
    }

    @Override
    @Test
    public void testMergeHyperLogLog()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT cardinality(merge(create_hll(custkey))) FROM orders", "Scalar function name not registered: presto.default.create_hll, called with arguments", true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupBy()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT orderstatus, cardinality(merge(create_hll(custkey))) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", "Scalar function name not registered: presto.default.create_hll, called with arguments", true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogWithNulls()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT cardinality(merge(create_hll(IF(orderstatus = 'O', custkey)))) FROM orders",
                "Scalar function name not registered: presto.default.create_hll, called with arguments", true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT orderstatus, cardinality(merge(create_hll(IF(orderstatus != 'O', custkey)))) " +
                        "FROM orders " +
                        "GROUP BY orderstatus",
                "Scalar function name not registered: presto.default.create_hll, called with arguments", true);
    }

    @Override
    @Test
    public void testKeyBasedSampling()
    {
        String[] queries = new String[] {
                "select count(1) from orders join lineitem using(orderkey)",
                "select count(1) from (select custkey, max(orderkey) from orders group by custkey)",
                "select count_if(m >= 1) from (select max(orderkey) over(partition by custkey) m from orders)",
                "select cast(m as bigint) from (select sum(totalprice) over(partition by custkey order by comment) m from orders order by 1 desc limit 1)",
                "select count(1) from lineitem where orderkey in (select orderkey from orders where length(comment) > 7)",
                "select count(1) from lineitem where orderkey not in (select orderkey from orders where length(comment) > 27)",
                "select count(1) from (select distinct orderkey, custkey from orders)",
        };
        int[] unsampledResults = new int[] {60175, 1000, 15000, 5408941, 60175, 9256, 15000};
        for (int i = 0; i < queries.length; i++) {
            assertQuery(queries[i], "select " + unsampledResults[i]);
        }

        Session sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .setSystemProperty(KEY_BASED_SAMPLING_PERCENTAGE, "0.2")
                .build();

        int[] sampled20PercentResults = new int[] {37170, 616, 9189, 5408941, 37170, 5721, 9278};
        for (int i = 0; i < queries.length; i++) {
            // TODO_PRESTISSIMO_FIX
            assertQueryFails(sessionWithKeyBasedSampling, queries[i], "Scalar function name not registered: presto.default.key_sampling_percent, called with arguments", true);
        }

        sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .setSystemProperty(KEY_BASED_SAMPLING_PERCENTAGE, "0.1")
                .build();

        int[] sampled10PercentResults = new int[] {33649, 557, 8377, 4644937, 33649, 5098, 8397};
        for (int i = 0; i < queries.length; i++) {
            // TODO_PRESTISSIMO_FIX
            assertQueryFails(sessionWithKeyBasedSampling, queries[i], "Scalar function name not registered: presto.default.key_sampling_percent, called with arguments", true);
        }
    }

    @Override
    @Test
    public void testP4ApproxSetBigint()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) FROM orders",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetVarchar()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) FROM orders",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetBigintGroupBy()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetDouble()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) FROM orders",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetDoubleGroupBy()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetGroupByWithNulls()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(IF(custkey % 2 <> 0, custkey)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(IF(orderstatus != 'O', custkey)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetOnlyNulls()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT cardinality(cast(approx_set(null) AS P4HYPERLOGLOG)) FROM orders",
                expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetVarcharGroupBy()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", expErrMsg1, true);
    }

    @Override
    @Test
    public void testP4ApproxSetWithNulls()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT cardinality(cast(approx_set(IF(orderstatus = 'O', custkey)) AS P4HYPERLOGLOG)) FROM orders",
                expErrMsg1, true);
    }

    @Override
    public void testLargeBytecode()
    {
        StringBuilder stringBuilder = new StringBuilder("SELECT x FROM (SELECT orderkey x, custkey y from orders limit 10) WHERE CASE true ");
        // Generate 100 cases.
        for (int i = 0; i < 100; i++) {
            stringBuilder.append(" when x in (");
            for (int j = 0; j < 20; j++) {
                stringBuilder.append("random(" + (i * 100 + j) + "), ");
            }

            stringBuilder.append("random(" + i + ")) then x = random()");
        }

        stringBuilder.append("else x = random() end");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(stringBuilder.toString(),
                "input > 0 \\(0 vs. 0\\) bound must be positive presto.default.random\\(0:INTEGER\\)", true);
    }

    @Override
    @Test
    public void testArrayCumSum()
    {
        // int
        String sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, 0]), (ARRAY[]), (CAST(NULL AS array(integer)))) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, 0]), (ARRAY[]), (CAST(NULL AS array(integer))), (ARRAY [cast(2147483647 as INTEGER), 2147483647, 2147483647])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, null, 2, 3])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        sql = "select array_cum_sum(k) from (values (array[cast(null as INTEGER), 6, null, 2, 3])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        // bigint
        sql = "select array_cum_sum(k) from (values (array[cast(5 as bigint), 6, 0]), (ARRAY[]), (CAST(NULL AS array(bigint))), (ARRAY [cast(2147483647 as bigint), 2147483647, 2147483647])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        sql = "select array_cum_sum(k) from (values (array[cast(5 as bigint), 6, null, 2, 3])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        sql = "select array_cum_sum(k) from (values (array[cast(null as bigint), 6, null, 2, 3])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        // real
        sql = "select array_cum_sum(k) from (values (array[cast(null as real), 6, null, 2, 3])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_cum_sum(k) FROM (values (ARRAY [cast(5.1 as real), 6.1, 0.5]), (ARRAY[]), (CAST(NULL AS array(real))), " +
                        "(ARRAY [cast(null as real), 6.1, 0.5]), (ARRAY [cast(2.5 as real), 6.1, null, 3.2])) t(k)",
                "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        // double
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT array_cum_sum(k) FROM (values (ARRAY [cast(5.1 as double), 6.1, 0.5]), (ARRAY[]), (CAST(NULL AS array(double))), " +
                        "(ARRAY [cast(null as double), 6.1, 0.5]), (ARRAY [cast(5.1 as double), 6.1, null, 3.2])) t(k)",
                "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        // decimal
        sql = "select array_cum_sum(k) from (values (array[cast(5.1 as decimal(38, 1)), 6, 0]), (ARRAY[]), (CAST(NULL AS array(decimal)))) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        sql = "select array_cum_sum(k) from (values (array[cast(5.1 as decimal(38, 1)), 6, null, 3]), (array[cast(null as decimal(38, 1)), 6, null, 3])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        // varchar
        sql = "select array_cum_sum(k) from (values (array[cast('5.1' as varchar), '6', '0']), (ARRAY[]), (CAST(NULL AS array(varchar)))) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);

        sql = "select array_cum_sum(k) from (values (array[cast(null as varchar), '6', '0'])) t(k)";
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, "Scalar function name not registered: presto.default.array_cum_sum, called with arguments:", true);
    }

    @Override
    @Test
    public void testApproxMostFrequentWithVarchar()
    {
        MaterializedResult actual1 = computeActual("SELECT approx_most_frequent(3, x, 15) FROM (values 'A', 'B', 'A', 'C', 'A', 'B', 'C', 'D', 'E') t(x)");
        assertEquals(actual1.getRowCount(), 1);
        // PRESTISSIMO_FIX
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of("A", 3L, "B", 2L, "C", 2L));

        MaterializedResult actual2 = computeActual("SELECT approx_most_frequent(2, x, 15) FROM (values 'A', 'B', 'A', 'C', 'A', 'B', 'C', 'D', 'E') t(x)");
        assertEquals(actual2.getRowCount(), 1);
        // PRESTISSIMO_FIX
        assertEquals(actual2.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of("A", 3L, "C", 2L));
    }

    @Override
    @Test
    public void testMapUnionSumOverflow()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,100] as array<tinyint>)) x " +
                        "union all select 1 y, map(array['x', 'y'], cast(array[1,100] as array<tinyint>))x) group by y", "Value 200 exceeds 127", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30, 32760] as array<smallint>)) x " +
                        "union all select 1 y, map(array['x', 'y'], cast(array[1,100] as array<smallint>))x) group by y", "Value 32860 exceeds 32767", true);
    }

    @Override
    @Test
    public void testTryInvalidCast()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TRY(CAST('a' AS BIGINT))",
                "Scalar function name not registered: presto.default.fail, called with arguments", true);
    }

    @Override
    @Test
    public void testRowSubscript()
    {
        // Subscript on Row with unnamed fields
        assertQuery("SELECT ROW (1, 'a', true)[2]", "SELECT 'a'");
        assertQuery("SELECT r[2] FROM (VALUES (ROW (ROW (1, 'a', true)))) AS v(r)", "SELECT 'a'");
        assertQuery("SELECT r[1], r[2] FROM (SELECT ROW (name, regionkey) FROM nation ORDER BY name LIMIT 1) t(r)", "VALUES ('ALGERIA', 0)");

        // Subscript on Row with named fields
        assertQuery("SELECT (CAST (ROW (1, 'a', 2 ) AS ROW (field1 bigint, field2 varchar(1), field3 bigint)))[2]", "SELECT 'a'");

        // Subscript on nested Row
        assertQuery("SELECT ROW (1, 'a', ROW (false, 2, 'b'))[3][3]", "SELECT 'b'");

        // Row subscript in filter condition
        assertQuery("SELECT orderstatus FROM orders WHERE ROW (orderkey, custkey)[1] = 100", "SELECT 'O'");

        // Row subscript in join condition
        assertQuery("SELECT n.name, r.name FROM nation n JOIN region r ON ROW (n.name, n.regionkey)[2] = ROW (r.name, r.regionkey)[2] ORDER BY n.name LIMIT 1", "VALUES ('ALGERIA', 'AFRICA')");

        //Row subscript in a lambda
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(ROW (1, 2), r -> r[2])", "Scalar function name not registered: presto.default.apply, called with arguments", true);
    }

    @Override
    public void testCorrelatedExistsSubqueries()
    {
        // projection
        assertQuery(
                "SELECT EXISTS(SELECT 1 FROM (VALUES 1, 1, 1, 2, 2, 3, 4) i(a) WHERE i.a < o.a AND i.a < 4) " +
                        "FROM (VALUES 0, 3, 3, 5) o(a)",
                "VALUES false, true, true, true");
        assertQuery(
                "SELECT EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) " +
                        "FROM lineitem l LIMIT 1");

// TODO_PRESTISSIMO_FIX - TODO: Query returns different output everytime.
//        assertQuery(
//                "SELECT count(*) FROM orders o " +
//                        "WHERE EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 1000 = 0)",
//                "VALUES 29934"); // h2 is slow
        assertQuery(
                "SELECT count(*) FROM lineitem l " +
                        "WHERE EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o ORDER BY " +
                        "EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "LIMIT 1",
                "VALUES 60000"); // h2 is slow
        assertQuery(
                "SELECT orderkey FROM lineitem l ORDER BY " +
                        "EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // group by
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey, " +
                        "EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) " +
                        "FROM orders o GROUP BY o.orderkey ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1, true)"); // h2 is slow
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey " +
                        "FROM orders o " +
                        "GROUP BY o.orderkey " +
                        "HAVING EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey FROM orders o " +
                        "GROUP BY o.orderkey, EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow
        assertQuery(
                "SELECT max(l.quantity), l.orderkey, EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) FROM lineitem l " +
                        "GROUP BY l.orderkey");
        assertQuery(
                "SELECT max(l.quantity), l.orderkey FROM lineitem l " +
                        "GROUP BY l.orderkey " +
                        "HAVING EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");
        assertQuery(
                "SELECT max(l.quantity), l.orderkey FROM lineitem l " +
                        "GROUP BY l.orderkey, EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT EXISTS(SELECT 1 FROM orders i WHERE o1.orderkey < o2.orderkey AND i.orderkey % 10000 = 0)");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT EXISTS(SELECT 1 FROM orders i WHERE o1.orderkey < o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
// TODO_PRESTISSIMO_FIX - TODO: Query returns different output everytime.
//        assertQuery(
//                "SELECT count(*) FROM orders o " +
//                        "WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)))",
//                "VALUES 14999"); // h2 is slow
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 WHERE o.orderkey > 10 OR o.orderkey != 3)))",
                "VALUES 14999");
    }

    // TODO_PRESTISSIMO_FIX - TODO: Some queries returns different output everytime.
    @Override
    @Test (enabled = false)
    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
    {
        // projection
        assertQuery(
                "SELECT (SELECT round(3 * avg(i.a)) FROM (VALUES 1, 1, 1, 2, 2, 3, 4) i(a) WHERE i.a < o.a AND i.a < 4) " +
                        "FROM (VALUES 0, 3, 3, 5) o(a)",
                "VALUES null, 4, 4, 5");

        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE (SELECT avg(i.orderkey) FROM orders i " +
                        "WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) > 100",
                "VALUES 14999"); // h2 is slow

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o " +
                        "ORDER BY " +
                        "   (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0), " +
                        "   orderkey " +
                        "LIMIT 1",
                "VALUES 1"); // h2 is slow

        // group by
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey, " +
                        "(SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) " +
                        "FROM orders o GROUP BY o.orderkey ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1, 40000)"); // h2 is slow
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey " +
                        "FROM orders o " +
                        "GROUP BY o.orderkey " +
                        "HAVING 40000 < (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-07-24', 20000)"); // h2 is slow
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey FROM orders o " +
                        "GROUP BY o.orderkey, (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT 1 = (SELECT avg(i.orderkey) FROM orders i WHERE o1.orderkey < o2.orderkey AND i.orderkey % 10000 = 0)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT 1 = (SELECT avg(i.orderkey) FROM orders i WHERE o1.orderkey < o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE 100 < (SELECT * " +
                        "FROM (SELECT (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)))",
                "VALUES 14999"); // h2 is slow

        // consecutive correlated subqueries with scalar aggregation
        assertQuery("SELECT " +
                "(SELECT avg(regionkey) " +
                " FROM nation n2" +
                " WHERE n2.nationkey = n1.nationkey)," +
                "(SELECT avg(regionkey)" +
                " FROM nation n3" +
                " WHERE n3.nationkey = n1.nationkey)" +
                "FROM nation n1");
        assertQuery("SELECT" +
                "(SELECT avg(regionkey)" +
                " FROM nation n2 " +
                " WHERE n2.nationkey = n1.nationkey)," +
                "(SELECT avg(regionkey)+1 " +
                " FROM nation n3 " +
                " WHERE n3.nationkey = n1.nationkey)" +
                "FROM nation n1");

        //count in subquery
        assertQuery("SELECT * " +
                        "FROM (VALUES (0),( 1), (2), (7)) AS v1(c1) " +
                        "WHERE v1.c1 > (SELECT count(c1) FROM (VALUES (0),( 1), (2)) AS v2(c1) WHERE v1.c1 = v2.c1)",
                "VALUES (2), (7)");
    }

    @Override
    @Test
    public void testDuplicateUnnestItem()
    {
        // unnest with cross join
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3]) AS r(r1, r2)", "VALUES (1, 2, 2), (1, 3, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) AS r(r1, r2, r3)", "VALUES (1, 2, 2, 2), (1, 3, 3, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) AS r(r1, r2, r3)", "VALUES (1, 2, 10, 2), (1, 3, 11, 3), (1, NULL, 12, NULL)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) AS r(r1, r2, r3)", "VALUES (1, 2, 2, 10), (1, 3, 3, 11), (1, NULL, NULL, 12)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) AS r(r1, r2, r3, r4)", "VALUES (1, 2, 'a', 2, 'a'), (1, 3, 'b', 3, 'b')");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) AS r(r1, r2, r3, r4, r5, r6)",
                "VALUES (1, 2, 'a', 1, 'a', 2, 'a'), (1, 3, 'b', 2, 'b', 3, 'b'), (1, NULL, NULL, 3, 'c', NULL, NULL)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'])) AS r(r1, r2, r3, r4, r5, r6)",
                "VALUES (1, 2, 'a', 2, 'a', 1, 'a'), (1, 3, 'b', 3, 'b', 2, 'b'), (1, NULL, NULL, NULL, NULL, 3, 'c')");
        assertQuery("SELECT * from ( SELECT ARRAY[1] AS kv FROM (select 1)) CROSS JOIN UNNEST( kv, kv )", "VALUES (ARRAY[1], 1, 1)");

        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)", "VALUES (1, 2, 2, 1), (1, 3, 3, 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 2, 2, 1), (1, 3, 3, 3, 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 10, 2, 1), (1, 3, 11, 3, 2), (1, NULL, 12, NULL, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 2, 10, 1), (1, 3, 3, 11, 2), (1, NULL, NULL, 12, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)", "VALUES (1, 2, 'a', 2, 'a', 1), (1, 3, 'b', 3, 'b', 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, ord)",
                "VALUES (1, 2, 'a', 1, 'a', 2, 'a', 1), (1, 3, 'b', 2, 'b', 3, 'b', 2), (1, NULL, NULL, 3, 'c', NULL, NULL, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'])) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, ord)",
                "VALUES (1, 2, 'a', 2, 'a', 1, 'a', 1), (1, 3, 'b', 3, 'b', 2, 'b', 2), (1, NULL, NULL, NULL, NULL, 3, 'c', 3)");
        assertQuery("SELECT * from ( SELECT ARRAY[1] AS kv FROM (select 1)) CROSS JOIN UNNEST( kv, kv ) WITH ORDINALITY AS t(r1, r2, ord)", "VALUES (ARRAY[1], 1, 1, 1)");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3, r4, r5, r6, r7)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, r7, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);

        Session useLegacyUnnest = Session.builder(getSession())
                .setSystemProperty(LEGACY_UNNEST, "true")
                .build();
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)) from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2)",
                "VALUES (1, row(2, 3), row(2, 3)), (1, row(3, 5), row(3, 5))");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)) from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3)",
                "VALUES (1, row(2, 3), row(2, 3), row(10, 13, 15)), (1, row(3, 5), row(3, 5), row(23, 25, 20))");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), ord from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, ord)",
                "VALUES (1, row(2, 3), row(2, 3), 1), (1, row(3, 5), row(3, 5), 2)");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)), ord from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, ord)",
                "VALUES (1, row(2, 3), row(2, 3), row(10, 13, 15), 1), (1, row(3, 5), row(3, 5), row(23, 25, 20), 2)");

        assertQuery("select orderkey, custkey, totalprice, t1, t2 from orders cross join unnest(array[custkey], array[custkey]) as t(t1, t2)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2 from orders");
        assertQuery("select orderkey, custkey, totalprice, t1, t2, t3 from orders cross join unnest(array[custkey], array[custkey], array[custkey]) as t(t1, t2, t3)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2, custkey as t3 from orders");
        assertQuery("select orderkey, custkey, totalprice, t1, t2, t3 from orders cross join unnest(array[custkey], array[custkey], array[shippriority]) as t(t1, t2, t3)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2, shippriority as t3 from orders");

        // SIMPLE UNNEST without cross join
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3])", "VALUES (2, 2), (3, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3])", "VALUES (2, 2, 2), (3, 3, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3])", "VALUES (2, 10, 2), (3, 11, 3), (NULL, 12, NULL)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12])", "VALUES (2, 2, 10), (3, 3, 11), (NULL, NULL, 12)");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']))", "VALUES (2, 'a', 2, 'a'), (3, 'b', 3, 'b')");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b']))",
                "VALUES (2, 'a', 1, 'a', 2, 'a'), (3, 'b', 2, 'b', 3, 'b'), (NULL, NULL, 3, 'c', NULL, NULL)");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']))",
                "VALUES (2, 'a', 2, 'a', 1, 'a'), (3, 'b', 3, 'b', 2, 'b'), (NULL, NULL, NULL, NULL, 3, 'c')");

        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)", "VALUES (2, 2, 1), (3, 3, 2)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 2, 2, 1), (3, 3, 3, 2)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 10, 2, 1), (3, 11, 3, 2), (NULL, 12, NULL, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 2, 10, 1), (3, 3, 11, 2), (NULL, NULL, 12, 3)");

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);

        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)) from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2)",
                "VALUES (row(2, 3), row(2, 3)), (row(3, 5), row(3, 5))");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)) from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3)",
                "VALUES (row(2, 3), row(2, 3), row(10, 13, 15)), (row(3, 5), row(3, 5), row(23, 25, 20))");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), ord from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, ord)",
                "VALUES (row(2, 3), row(2, 3), 1), (row(3, 5), row(3, 5), 2)");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)), ord from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, ord)",
                "VALUES (row(2, 3), row(2, 3), row(10, 13, 15), 1), (row(3, 5), row(3, 5), row(23, 25, 20), 2)");

        // mixed
        assertQuery("select * from (SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)) cross join unnest(ARRAY[2, 3], ARRAY[2, 3])",
                "VALUES (2, 2, 1, 2, 2), (2, 2, 1, 3, 3), (3, 3, 2, 2, 2), (3, 3, 2, 3, 3)");
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSet()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey) c FROM orders UNION ALL SELECT empty_approx_set())",
                "Scalar function name not registered: presto.default.create_hll, called with arguments: \\(BIGINT\\)", true);
    }

    @Override
    @Test
    public void testSetUnion()
    {
        // sanity
        assertQuery(
                "select set_union(x) from (values array[1, 2], array[3, 4], array[5, 6]) as t(x)",
                "select array[1, 2, 3, 4, 5, 6]");
        assertQuery(
                "select set_union(x) from (values array[1, 2, 3], array[2, 3, 4], array[7, 8]) as t(x)",
                "select array[1, 2, 3, 4, 7, 8]");
        assertQuery(
                "select group_id, set_union(numbers) from (values (1, array[1, 2]), (1, array[2, 3]), (2, array[4, 5]), (2, array[5, 6])) as t(group_id, numbers) group by group_id",
                "select group_id, numbers from (values (1, array[1, 2, 3]), (2, array[4, 5, 6])) as t(group_id, numbers)");
        assertQuery(
                "select group_id, set_union(numbers) from (values (1, array[1, 2]), (2, array[2, 3]), (3, array[4, 5]), (4, array[5, 6])) as t(group_id, numbers) group by group_id",
                "select group_id, numbers from (values (1, array[1, 2]), (2, array[2, 3]), (3, array[4, 5]), (4, array[5, 6])) as t(group_id, numbers)");
        // TODO_PRESTISSIMO_FIX
        // all nulls should return empty array to match behavior of array_distinct(flatten(array_agg(x)))
        assertQueryFails(
                "select set_union(x) from (values null, null, null) as t(x)",
                "Unexpected type UNKNOWN", true);
        // TODO_PRESTISSIMO_FIX
        // nulls inside arrays should be captured while pure nulls should be ignored
        assertQueryFails(
                "select set_union(x) from (values null, array[null], null) as t(x)",
                "Unexpected type UNKNOWN", true);
        // TODO_PRESTISSIMO_FIX
        // null inside arrays should be captured
        assertQuery(
                "select set_union(x) from (values array[1, 2, 3], array[null], null) as t(x)",
                "select array[1, 2, 3, null]");
        // TODO_PRESTISSIMO_FIX
        // return null for empty rows
        assertQueryFails(
                "select set_union(x) from (values null, array[null], null) as t(x) where x != null",
                "Unexpected type UNKNOWN", true);
    }

    @Override
    @Test
    public void testSamplingJoinChain()
    {
        Session sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .build();
        String query = "select count(1) FROM lineitem l left JOIN orders o ON l.orderkey = o.orderkey JOIN customer c ON o.custkey = c.custkey";

        assertQuery(query, "select 60175");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sessionWithKeyBasedSampling, query, "Scalar function name not registered: presto.default.key_sampling_percent, called with arguments", true);
    }

    @Override
    @Test
    public void testNestedCast()
    {
        // TODO_PRESTISSIMO_FIX
        assertQuery("select cast(varchar_value as varchar(3)) || ' sfd' from (values ('9898.122')) t(varchar_value)", "VALUES '9898.122 sfd'");
        // TODO_PRESTISSIMO_FIX
        assertQuery("select cast(cast(varchar_value as varchar(3)) as varchar(5)) from (values ('9898.122')) t(varchar_value)", "VALUES '9898.122'");
    }

    @Override
    @Test
    public void testApproxSetBigint()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(custkey)) FROM orders");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1005L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetBigintGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(custkey)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1003L)
                .row("F", 1001L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetDouble()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS DOUBLE))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetDoubleGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS DOUBLE))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1002L)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetGroupByWithNulls()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(custkey % 2 <> 0, custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 499L)
                .row("F", 496L)
                .row("P", 153L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetVarchar()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS VARCHAR))) FROM orders");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1015L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetVarcharGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS VARCHAR))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1012L)
                .row("F", 1011L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetWithNulls()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(IF(orderstatus = 'O', custkey))) FROM orders");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1003L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(orderstatus != 'O', custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 1001L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testReduceAggWithNulls()
    {
        assertQueryFails("select reduce_agg(x, null, (x,y)->try(x+y), (x,y)->try(x+y)) from (select 1 union all select 10) T(x)", ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");
        assertQueryFails("select reduce_agg(x, cast(null as bigint), (x,y)->coalesce(x, 0)+coalesce(y, 0), (x,y)->coalesce(x, 0)+coalesce(y, 0)) from (values cast(10 as bigint),10)T(x)", ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");

        // here some reduce_aggs coalesce overflow/zero-divide errors to null in the input/combine functions
        assertQuery("select reduce_agg(x, 0, (x,y)->try(1/x+1/y), (x,y)->try(1/x+1/y)) from ((select 0) union all select 10.) T(x)", "select 0.0");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select reduce_agg(x, 0, (x, y)->try(x+y), (x, y)->try(x+y)) from (values 2817, 9223372036854775807) AS T(x)", "!states->isNullAt\\(i\\) Lambda expressions in reduce_agg should not return null for non-null inputs", true);
        assertQuery("select reduce_agg(x, array[], (x, y)->array[element_at(x, 2)],  (x, y)->array[element_at(x, 2)]) from (select array[array[1]]) T(x)", "select array[null]");
    }

    @Override
    // TODO_PRESTISSIMO_FIX
    @Test(expectedExceptions = {RuntimeException.class, PrestoException.class}, expectedExceptionsMessageRegExp = ".*Scalar function name not registered: presto.default.create_hll, called with arguments.*")
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError()
    {
        computeActual("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.2))");
    }

    //@Test @Override
    @Override
    // TODO_PRESTISSIMO_FIX
    @Test(expectedExceptions = {RuntimeException.class, PrestoException.class}, expectedExceptionsMessageRegExp = ".*Scalar function name not registered: presto.default.create_hll, called with arguments.*")
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.1))");
        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1046L)
                .build();
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testRows()
    {
        // Using JSON_FORMAT(CAST(_ AS JSON)) because H2 does not support ROW type
        Session session = Session.builder(getSession()).setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true").build();
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS JSON))", "SELECT '{\"\":3,\"\":\"ab\"}'");
        // TODO_PRESTISSIMO_FIX
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(a + b) AS JSON)) FROM (VALUES (1, 2)) AS t(a, b)", "SELECT '[3]'");
        // TODO_PRESTISSIMO_FIX
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(1, ROW(9, a, ARRAY[], NULL), ROW(1, 2)) AS JSON)) FROM (VALUES ('a')) t(a)",
                "SELECT '[1,[9,\"a\",[],null],[1,2]]'");
        // TODO_PRESTISSIMO_FIX
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(ROW(ROW(ROW(ROW(a, b), c), d), e), f) AS JSON)) FROM (VALUES (ROW(0, 1), 2, '3', NULL, ARRAY[5], ARRAY[])) t(a, b, c, d, e, f)",
                "SELECT '[[[[[[0,1],2],\"3\"],null],[5]],[]]'");
        // TODO_PRESTISSIMO_FIX
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(a, b)) AS JSON)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)",
                "SELECT '[[1,2],[3,4],[5,6]]'");
        assertQuery(session, "SELECT CONTAINS(ARRAY_AGG(ROW(a, b)), ROW(1, 2)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)", "SELECT TRUE");
        // TODO_PRESTISSIMO_FIX
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(c, d)) AS JSON)) FROM (VALUES (ARRAY[1, 3, 5], ARRAY[2, 4, 6])) AS t(a, b) CROSS JOIN UNNEST(a, b) AS u(c, d)",
                "SELECT '[[1,2],[3,4],[5,6]]'");
        // TODO_PRESTISSIMO_FIX
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, NULL, '3')) t(x,y,z)", "SELECT '[1,null,\"3\"]'");
        // TODO_PRESTISSIMO_FIX
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, CAST(NULL AS INTEGER), '3')) t(x,y,z)", "SELECT '[1,null,\"3\"]'");
    }

    @Override
    public void testCorrelatedNonAggregationScalarSubqueries()
    {
        String subqueryReturnedTooManyRows = "Scalar sub-query has returned multiple rows";

        assertQuery("SELECT (SELECT 1 WHERE a = 2) FROM (VALUES 1) t(a)", "SELECT null");
        assertQuery("SELECT (SELECT 2 WHERE a = 1) FROM (VALUES 1) t(a)", "SELECT 2");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT (SELECT 2 FROM (VALUES 3, 4) WHERE a = 1) FROM (VALUES 1) t(a)", expErrMsg3, true);

        // multiple subquery output projections
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT 'bleh' FROM region WHERE regionkey > n.regionkey)",
                 expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey > n.regionkey)",
                 expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT name FROM nation n WHERE 1 = (SELECT 1 FROM region WHERE regionkey > n.regionkey)",
                 expErrMsg3, true);

        // correlation used in subquery output
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT n.name FROM region WHERE regionkey > n.regionkey)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        assertQuery(
                "SELECT (SELECT 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                "VALUES 2, null, null, null, null");
        // outputs plain correlated orderkey symbol which causes ambiguity with outer query orderkey symbol
        assertQueryFails(
                "SELECT (SELECT o.orderkey WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertQueryFails(
                "SELECT (SELECT o.orderkey * 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // correlation used outside the subquery
        assertQueryFails(
                "SELECT o.orderkey, (SELECT o.orderkey * 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // aggregation with having
//        TODO: uncomment below test once #8456 is fixed
//        assertQuery("SELECT (SELECT avg(totalprice) FROM orders GROUP BY custkey, orderdate HAVING avg(totalprice) < a) FROM (VALUES 900) t(a)");

        // correlation in predicate
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey = n.regionkey)", expErrMsg3, true);

        // same correlation in predicate and projection
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT n.regionkey * 2 FROM region r WHERE n.regionkey = r.regionkey) > 6",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // different correlation in predicate and projection
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT n.nationkey * 2 FROM region r WHERE n.regionkey = r.regionkey) > 6",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // correlation used in subrelation
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT regionkey * 2 FROM (SELECT regionkey FROM region r WHERE n.regionkey = r.regionkey)) > 6 " +
                        "ORDER BY 1 LIMIT 3", expErrMsg3, true);

        // with duplicated rows
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 1, 1, 2, 3) t(a)", expErrMsg3, true);

        // returning null when nothing matched
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 31) t(a)", expErrMsg3, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT (SELECT r.name FROM nation n, region r WHERE r.regionkey = n.regionkey AND n.nationkey = a) FROM (VALUES 1) t(a)", expErrMsg3, true);
    }

    @Override
    public void testCorrelatedScalarSubqueries()
    {
        assertQuery("SELECT (SELECT n.nationkey + n.NATIONKEY) FROM nation n");
        assertQuery("SELECT (SELECT 2 * n.nationkey) FROM nation n");
        assertQuery("SELECT nationkey FROM nation n WHERE 2 = (SELECT 2 * n.nationkey)");
        assertQuery("SELECT nationkey FROM nation n ORDER BY (SELECT 2 * n.nationkey)");

        // group by
        assertQuery("SELECT max(n.regionkey), 2 * n.nationkey, (SELECT n.nationkey) FROM nation n GROUP BY n.nationkey");
        assertQuery(
                "SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING max(l.quantity) < (SELECT l.orderkey)");
        assertQuery("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey, (SELECT l.orderkey)");

        // join
        assertQuery("SELECT * FROM nation n1 JOIN nation n2 ON n1.nationkey = (SELECT n2.nationkey)");
        assertQueryFails(
                "SELECT (SELECT l3.* FROM lineitem l2 CROSS JOIN (SELECT l1.orderkey) l3 LIMIT 1) FROM lineitem l1",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // subrelation
        assertQuery(
                "SELECT 1 FROM nation n WHERE 2 * nationkey - 1  = (SELECT * FROM (SELECT n.nationkey))",
                "SELECT 1"); // h2 fails to parse this query

        // two level of nesting
        assertQuery("SELECT * FROM nation n WHERE 2 = (SELECT (SELECT 2 * n.nationkey))");

        // explicit LIMIT in subquery
        assertQueryFails(
                "SELECT (SELECT count(*) FROM (VALUES (7,1)) t(orderkey, value) WHERE orderkey = corr_key LIMIT 1) FROM (values 7) t(corr_key)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "SELECT (SELECT count(*) FROM (VALUES (7,1)) t(orderkey, value) WHERE orderkey = corr_key GROUP BY value LIMIT 2) FROM (values 7) t(corr_key)",
                 expErrMsg3, true);

        // Limit(1) and non-constant output symbol of the subquery (count)
        assertQueryFails("SELECT (SELECT count(*) FROM (VALUES (7,1), (7,2)) t(orderkey, value) WHERE orderkey = corr_key GROUP BY value LIMIT 1) FROM (values 7) t(corr_key)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Override
    public void testLikePrefixAndSuffixWithChars()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select x like 'abc%' from (values CAST ('abc' AS CHAR(3)), CAST ('def' AS CHAR(3)), CAST ('bcd' AS CHAR(3))) T(x)", expErrMsg5, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select x like '%abc%' from (values CAST ('xabcy' AS CHAR(5)), CAST ('abxabcdef' AS CHAR(9)), CAST ('bcd' AS CHAR(3)),  CAST ('xabcyabcz' AS CHAR(9))) T(x)", expErrMsg5, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(
                "select x like '%abc' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4)), CAST ('xabc' AS CHAR(4)), CAST (' xabc' AS CHAR(5))) T(x)", expErrMsg5, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select x like '%ab_c' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", expErrMsg5, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select x like '%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", expErrMsg5, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select x like '%_%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", expErrMsg5, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select x like '%a%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", expErrMsg5, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select x like '%acd%xy%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", expErrMsg5, true);
    }

    @Override
    public void testMapBlockBug()
    {
        assertQueryFails(" VALUES(MAP_AGG(12345,123))", "Scalar function name not registered: presto.default.map_agg", true);
    }

    @Override
    public void testMergeKHyperLogLog()
    {
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select k1, cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                        "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2) group by k1", expErrMsg4, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                        "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2)", expErrMsg4, true);
    }

    @Override
    public void testQueryWithEmptyInput()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "true")
                .build();

        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o left join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from (select orderkey, linenumber from lineitem where false) l right join orders o on l.orderkey = o.orderkey");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem union all select orderkey, custkey as partkey from orders where false");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem where orderkey in (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem where orderkey not in (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select count(*) as count from (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select orderkey, count(*) as count from (select orderkey from orders where false) group by orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o join (select orderkey, max(linenumber) as linenumber from lineitem where false group by orderkey) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select orderkey from orders where orderkey = (select orderkey from lineitem where false)");
        assertQuery(enableOptimization, "SELECT (SELECT 1 FROM orders WHERE false LIMIT 1) FROM lineitem");
        assertQuery(enableOptimization, "SELECT (SELECT 1 FROM orders WHERE false LIMIT 1) FROM lineitem");
        assertQuery(enableOptimization, "WITH emptyorders as (select orderkey, totalprice, orderdate from orders where false) SELECT orderkey, orderdate, totalprice, " +
                "ROW_NUMBER() OVER (ORDER BY orderdate) as row_num FROM emptyorders WHERE totalprice > 10 ORDER BY orderdate ASC LIMIT 10");
        // TODO_PRESTISSIMO_FIX - Env related issue should succeed with datafile in PARQUET format.
        assertQueryFails(enableOptimization, "WITH emptyorders as (select * from orders where false) SELECT p.name, l.orderkey, l.partkey, l.quantity, RANK() OVER (PARTITION BY p.name ORDER BY l.quantity DESC) AS rank_quantity " +
                "FROM lineitem l JOIN emptyorders o ON l.orderkey = o.orderkey JOIN part p ON l.partkey = p.partkey WHERE o.orderdate BETWEEN DATE '1995-03-01' AND DATE '1995-03-31' " +
                "AND l.shipdate BETWEEN DATE '1995-03-01' AND DATE '1995-03-31' AND p.size = 15 ORDER BY p.name, rank_quantity LIMIT 100", "Cannot check if varchar is BETWEEN date and date", true);
        assertQuery(enableOptimization, "select orderkey, row_number() over (partition by orderpriority), orderpriority from (select orderkey, orderpriority from orders where false)");
        assertQuery(enableOptimization, "select * from (select orderkey, row_number() over (partition by orderpriority order by orderkey) row_number, orderpriority from (select orderkey, orderpriority from orders where false)) where row_number < 2");

        emptyJoinQueries(enableOptimization);
    }
    private void emptyJoinQueries(Session session)
    {
        // Empty predicate and inner join
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // Empty non-null producing side for outer join.
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT left outer join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // 3 way join with empty non-null producing side for outer join
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT"
                        + " left outer join customer C1 on DT.custkey=C1.custkey"
                        + " left outer join customer C2 on C1.custkey=C2.custkey",
                "select 1 from orders where 1 =0");

        // Zero limit
        assertQuery(session, "select 1 from (select * from orders LIMIT 0) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // Negative test.
        assertQuery(session, "select 1 from (select * from orders) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders");

        // Empty null producing side for outer join.
        assertQuery(session, "select 1 from (select * from orders) ORD left outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select 1 from orders");

        // Empty null producing side for left outer join with constant field.
        assertQuery(session, "select One from (select * from orders) ORD left outer join (select 1 as One, custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select null as One from orders");

        // Empty null producing side for right outer join with constant field.
        assertQuery(session, "select One from (select 1 as One, custkey from customer where 1=0) CUST right outer join (select * from orders) ORD " +
                        " ON ORD.custkey=CUST.custkey",
                "select null as One from orders");

        // 3 way join with mix of left and right outer joins. DT left outer join C1 right outer join O2.
        // DT is empty which produces DT right outer join O2 which produce O2 as final result.
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT"
                        + " left outer join customer C1 on DT.custkey=C1.custkey"
                        + " right outer join orders O2 on C1.custkey=O2.custkey",
                "select 1 from orders");

        // Empty side for full outer join.
        assertQuery(session, "select 1 from (select * from orders) ORD full outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select 1 from orders");

        // Empty side for full outer join as input to aggregation.
        assertQuery(session, "select count(*), orderkey from (select * from orders) ORD full outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey group by orderkey order by orderkey",
                "select count(*), orderkey from orders group by orderkey order by orderkey");
    }

    @Override
    public void testReduceAgg()
    {
        assertQuery(
                "SELECT x, reduce_agg(y, 1, (a, b) -> a * b, (a, b) -> a * b) " +
                        "FROM (VALUES (1, 5), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 5 * 6 * 7), (2, 8 * 9), (3, 10)");
        assertQuery(
                "SELECT x, reduce_agg(y, 0, (a, b) -> a + b, (a, b) -> a + b) " +
                        "FROM (VALUES (1, 5), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 5 + 6 + 7), (2, 8 + 9), (3, 10)");

        assertQuery(
                "SELECT x, reduce_agg(y, 1, (a, b) -> a * b, (a, b) -> a * b) " +
                        "FROM (VALUES (1, CAST(5 AS DOUBLE)), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, CAST(5 AS DOUBLE) * 6 * 7), (2, 8 * 9), (3, 10)");
        assertQuery(
                "SELECT x, reduce_agg(y, 0, (a, b) -> a + b, (a, b) -> a + b) " +
                        "FROM (VALUES (1, CAST(5 AS DOUBLE)), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, CAST(5 AS DOUBLE) + 6 + 7), (2, 8 + 9), (3, 10)");

        assertQuery(
                "SELECT " +
                        "x, " +
                        "array_join(" +
                        "   array_sort(" +
                        "       split(reduce_agg(y, '', (a, b) -> a || b, (a, b) -> a || b), '')" +
                        "   ), " +
                        "   ''" +
                        ") " +
                        "FROM (VALUES (1, 'a'), (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e'), (3, 'f')) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 'abc'), (2, 'de'), (3, 'f')");

        assertQuery(
                "SELECT " +
                        "x, " +
                        "array_join(" +
                        "   array_sort(" +
                        "       array['x'] || reduce_agg(y, ARRAY[''], (a, b) -> a || b, (a, b) -> a || b)" +
                        "   ), " +
                        "   ''" +
                        ") " +
                        "FROM (VALUES (1, ARRAY['a']), (1, ARRAY['b']), (1, ARRAY['c']), (2, ARRAY['d']), (2, ARRAY['e']), (3, ARRAY['f'])) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 'abcx'), (2, 'dex'), (3, 'fx')");

        assertQuery("SELECT REDUCE_AGG((x,y), (0,0), (x, y)->(x[1],y[1]), (x,y)->(x[1],y[1]))[1] from (select 1 x, 2 y)", "select 0");
    }

    @Override
    public void testRemoveMapCast()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .build();
        assertQuery(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)",
                "values 0.5, 0.1");
        assertQuery(enableOptimization, "select element_at(feature, key) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)",
                "values 0.5, 0.1");
        assertQuery(enableOptimization, "select element_at(feature, key) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                "values 0.5, null");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                 "Cannot cast BIGINT.*to INTEGER. Overflow during arithmetic conversion", true);
        assertQuery(enableOptimization, "select feature[key] from (values (map(array[cast(1 as varchar), '2', '3', '4'], array[0.3, 0.5, 0.9, 0.1]), cast('2' as varchar)), (map(array[cast(1 as varchar), '2', '3', '4'], array[0.3, 0.5, 0.9, 0.1]), '4')) t(feature,  key)",
                "values 0.5, 0.1");
    }

    @Override
    @Test(enabled = false)
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "false")
                .build();
        // Trigger optimization
        assertQuery(session, "select * from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select o.orderkey, c.name from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select *, cast(o.custkey as varchar) from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select *, cast(o.custkey as varchar), cast(c.custkey as varchar) from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select r.custkey, r.orderkey, r.name, n.nationkey from (select o.custkey, o.orderkey, c.name from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)) r, nation n");
        // Do not trigger optimization
        assertQuery(session, "select * from customer c join orders o on cast(acctbal as varchar) = cast(totalprice as varchar)");
    }

    @Override
    public void testSameAggregationWithAndWithoutFilter()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "true")
                .build();
        Session disableOptimization = Session.builder(getSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "false")
                .build();

        // TODO_PRESTISSIMO_FIX - Disabled this query because it gets different results on machines having different architecture.
        // assertQuery(enableOptimization, "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey", "values (3,-6799976246779207259,5),(2,5,5),(0,-6799976246779207262,5),(4,-6799976246779207261,5),(1,-6799976246779207260,5)");
        assertQuery(enableOptimization, "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation", "values (15,25)");
        assertQuery(enableOptimization, "select count(1), count(1) filter (where k > 5) from (values 1, null, 3, 5, null, 8, 10) t(k)", "values (7, 2)");

        String sql = "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey";
        MaterializedResult resultWithOptimization = computeActual(enableOptimization, sql);
        MaterializedResult resultWithoutOptimization = computeActual(disableOptimization, sql);
        // TODO_PRESTISSIMO_FIX - Uncomment this once issue # 22429 is fixed.
//        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        sql = "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        sql = "select partkey, sum(quantity), sum(quantity) filter (where discount > 0.1) from lineitem group by grouping sets((), (partkey))";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);

        // multiple aggregations in query
        sql = "select partkey, sum(quantity), sum(quantity) filter (where discount < 0.05), sum(linenumber), sum(linenumber) filter (where discount < 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregations in multiple levels
        sql = "select partkey, avg(sum), avg(sum) filter (where tax < 0.05), avg(filtersum) from (select partkey, suppkey, sum(quantity) sum, sum(quantity) filter (where discount > 0.05) filtersum, max(tax) tax from lineitem where partkey=1598 group by partkey, suppkey) t group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // global aggregation
        sql = "select sum(quantity), sum(quantity) filter (where discount < 0.05) from lineitem";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // order by
        sql = "select partkey, array_agg(suppkey order by suppkey), array_agg(suppkey order by suppkey) filter (where discount > 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // grouping sets
        sql = "SELECT partkey, suppkey, sum(quantity), sum(quantity) filter (where discount > 0.05) from lineitem group by grouping sets((), (partkey), (partkey, suppkey))";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregation over union
        sql = "SELECT partkey, sum(quantity), sum(quantity) filter (where orderkey > 0) from (select quantity, orderkey, partkey from lineitem union all select totalprice as quantity, orderkey, custkey as partkey from orders) group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregation over join
        sql = "select custkey, sum(quantity), sum(quantity) filter (where tax < 0.05) from lineitem l join orders o on l.orderkey=o.orderkey group by custkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
    }

    @Override
    public void testScalarSubquery()
    {
        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // TODO_PRESTISSIMO_FIX - Env related issue. Disabled temporarily.
        // aggregation
        //assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
        //        "(SELECT max(orderkey) FROM orders)");

        // no output
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT orderkey FROM orders WHERE 0=1)");

        // TODO_PRESTISSIMO_FIX - Env related issue. Disabled temporarily.
        // no output matching with null test
        //assertQuery("SELECT * FROM lineitem WHERE \n" +
        //        "(SELECT orderkey FROM orders WHERE 0=1) " +
        //        "is null");
        assertQuery("SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is not null");

        // subquery results and in in-predicate
        assertQuery("SELECT (SELECT 1) IN (1, 2, 3)");
        assertQuery("SELECT (SELECT 1) IN (   2, 3)");

        // multiple subqueries
        assertQuery("SELECT (SELECT 1) = (SELECT 3)");
        assertQuery("SELECT (SELECT 1) < (SELECT 3)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "(SELECT min(orderkey) FROM orders)" +
                "<" +
                "(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT (SELECT 1), (SELECT 2), (SELECT 3)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE orderkey BETWEEN" +
                "   (SELECT avg(orderkey) FROM orders) - 10 " +
                "   AND" +
                "   (SELECT avg(orderkey) FROM orders) + 10");

        // subqueries with joins
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM orders o1 " +
                "INNER JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 " +
                "LEFT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM orders o1 RIGHT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT DISTINCT COUNT(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 " +
                        "FULL JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                        "ON o1.orderkey " +
                        "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                        "GROUP BY o1.orderkey",
                "VALUES 1, 10");

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY (SELECT 2)");

        // subquery returns multiple rows
        String multipleRowsErrorMsg = ".*Expected single row of input.*";
        assertQueryFails("SELECT * FROM lineitem WHERE orderkey = (\n" +
                        "SELECT orderkey FROM orders ORDER BY totalprice)",
                multipleRowsErrorMsg);
        assertQueryFails("SELECT orderkey, totalprice FROM orders ORDER BY (VALUES 1, 2)",
                multipleRowsErrorMsg);

        // exposes a bug in optimize hash generation because EnforceSingleNode does not
        // support more than one column from the underlying query
        assertQuery("SELECT custkey, (SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 1) FROM orders");

        // cast scalar sub-query
        assertQuery("SELECT 1.0/(SELECT 1), CAST(1.0 AS REAL)/(SELECT 1), 1/(SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1) AND 1 = (SELECT 1), 2.0 = (SELECT 1) WHERE 1.0 = (SELECT 1) AND 1 = (SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1), 2.0 = (SELECT 1), CAST(2.0 AS REAL) = (SELECT 1) WHERE 1.0 = (SELECT 1)");

        // coerce correlated symbols
        assertQuery("SELECT * FROM (VALUES 1) t(a) WHERE 1=(SELECT count(*) WHERE 1.0 = a)", "SELECT 1");
        assertQuery("SELECT * FROM (VALUES 1.0) t(a) WHERE 1=(SELECT count(*) WHERE 1 = a)", "SELECT 1.0");
    }

    @Override
    @Test(enabled = false)
    public void testShowColumns()
    {
    }

    @Override
    public void testTry()
    {
        // divide by zero
        assertQuery(
                "SELECT linenumber, sum(TRY(100/(CAST (tax*10 AS BIGINT)))) FROM lineitem GROUP BY linenumber",
                "SELECT linenumber, sum(100/(CAST (tax*10 AS BIGINT))) FROM lineitem WHERE CAST(tax*10 AS BIGINT) <> 0 GROUP BY linenumber");

        // invalid cast
        assertQuery(
                "SELECT TRY(CAST(IF(round(totalprice) % 2 = 0, CAST(totalprice AS VARCHAR), '^&$' || CAST(totalprice AS VARCHAR)) AS DOUBLE)) FROM orders",
                "SELECT CASE WHEN round(totalprice) % 2 = 0 THEN totalprice ELSE null END FROM orders");

        // invalid function argument
        assertQuery(
                "SELECT COUNT(TRY(to_base(100, CAST(round(totalprice/100) AS BIGINT)))) FROM orders",
                "SELECT SUM(CASE WHEN CAST(round(totalprice/100) AS BIGINT) BETWEEN 2 AND 36 THEN 1 ELSE 0 END) FROM orders");

        // as part of a complex expression
        assertQuery(
                "SELECT COUNT(CAST(orderkey AS VARCHAR) || TRY(to_base(100, CAST(round(totalprice/100) AS BIGINT)))) FROM orders",
                "SELECT SUM(CASE WHEN CAST(round(totalprice/100) AS BIGINT) BETWEEN 2 AND 36 THEN 1 ELSE 0 END) FROM orders");

        // missing function argument
        assertQueryFails("SELECT TRY()", "line 1:8: The 'try' function must have exactly one argument");

        // check that TRY is not pushed down
        assertQueryFails("SELECT TRY(x) IS NULL FROM (SELECT 1/y AS x FROM (VALUES 1, 2, 3, 0, 4) t(y))", ".*division by zero Top-level Expression.*presto.default.divide.*");
        assertQuery("SELECT x IS NULL FROM (SELECT TRY(1/y) AS x FROM (VALUES 3, 0, 4) t(y))", "VALUES false, true, false");

        // test try with lambda function
        // TODO_PRESTISSIMO_FIX -
        assertQueryFails("SELECT TRY(apply(5, x -> x + 1) / 0)", "Scalar function name not registered: presto.default.apply", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TRY(apply(5 + RANDOM(1), x -> x + 1) / 0)", "Scalar function name not registered: presto.default.apply", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT apply(5 + RANDOM(1), x -> x + TRY(1 / 0))", expErrMsg3, true);

        // test try with invalid JSON
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT JSON_FORMAT(TRY(JSON 'INVALID'))", expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT JSON_FORMAT(TRY (JSON_PARSE('INVALID')))", expErrMsg3, true);

        // tests that might be constant folded
        assertQuery("SELECT TRY(CAST(NULL AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT TRY(CAST('123' AS BIGINT))", "SELECT 123L");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TRY(CAST('foo' AS BIGINT))", expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TRY(CAST('foo' AS BIGINT)) + TRY(CAST('123' AS BIGINT))", expErrMsg3, true);
        assertQuery("SELECT TRY(CAST(CAST(123 AS VARCHAR) AS BIGINT))", "SELECT 123L");
        assertQuery("SELECT COALESCE(CAST(CONCAT('123', CAST(123 AS VARCHAR)) AS BIGINT), 0)", "SELECT 123123L");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TRY(CAST(CONCAT('hello', CAST(123 AS VARCHAR)) AS BIGINT))", expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT COALESCE(TRY(CAST(CONCAT('a', CAST(123 AS VARCHAR)) AS INTEGER)), 0)", expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT COALESCE(TRY(CAST(CONCAT('a', CAST(123 AS VARCHAR)) AS BIGINT)), 0)", expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT 123 + TRY(ABS(-9223372036854775807 - 1))", expErrMsg3, true);
        assertQuery("SELECT JSON_FORMAT(TRY(JSON '[]')) || '123'", "SELECT '[]123'");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT JSON_FORMAT(TRY(JSON 'INVALID')) || '123'", expErrMsg3, true);
        assertQuery("SELECT TRY(2/1)", "SELECT 2");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TRY(2/0)", expErrMsg3, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT COALESCE(TRY(2/0), 0)", expErrMsg3, true);
        assertQuery("SELECT TRY(ABS(-2))", "SELECT 2");

        // test try with null
        assertQuery("SELECT TRY(1 / x) FROM (SELECT NULL as x)", "SELECT NULL");
    }

    @Test
    public void testLambdaInAggregation()
    {
        assertQuery("SELECT id, reduce_agg(value, 0, (a, b) -> a + b+0, (a, b) -> a + b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id", "values (1, 9), (2, 90)");
        // TODO_PRESTISSIMO_FIX
        assertQuery("SELECT id, 's' || reduce_agg(value, '', (a, b) -> concat(a, b, 's'), (a, b) -> concat(a, b, 's')) FROM ( VALUES (1, '2'), (1, '3'), (1, '4'), (2, '20'), (2, '30'), (2, '40') ) AS t(id, value) GROUP BY id",
                "values (1, 's2s3ss4ss'), (2, 's20s30ss40ss')");
        assertQueryFails("SELECT id, reduce_agg(value, array[id, value], (a, b) -> a || b, (a, b) -> a || b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id",
                ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");
    }
}
