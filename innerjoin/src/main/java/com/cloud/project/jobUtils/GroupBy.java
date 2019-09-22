package com.cloud.project.jobUtils;

import com.cloud.project.contracts.DBManager;
import com.cloud.project.models.OutputModel;
import com.cloud.project.sqlUtils.AggregateFunction;
import com.cloud.project.sqlUtils.ParseSQL;
import com.cloud.project.sqlUtils.Tables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

public class GroupBy {

    /**
     * Executes the Hadoop Map-Reduce job for a Group By query
     *
     * @param parsedSQL instance of {@link ParseSQL} which contains the relevant tokens from the parsed SQL query
     * @return an instance of {@link OutputModel} populated with relevant fields from Hadoop execution
     * @throws IOException            if Hadoop IO fails
     * @throws InterruptedException   if Hadoop job is interrupted
     * @throws ClassNotFoundException if Hadoop environment fails to find the relevant class
     * @throws SQLException           if the SQL query could not be parsed successfully
     */
    public static OutputModel execute(ParseSQL parsedSQL) throws IOException,
            InterruptedException, ClassNotFoundException, SQLException {

        OutputModel groupByOutput = new OutputModel();

        Configuration conf = new Configuration();

        // like defined in hdfs-site.xml (required for reading file from hdfs)
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        // defining properties to be used later by mapper and reducer
        conf.setEnum("table", parsedSQL.getTable1());
        conf.setEnum("aggregateFunction", parsedSQL.getAggregateFunction());
        conf.setInt("comparisonNumber", parsedSQL.getComparisonNumber());
        conf.setStrings("columns", parsedSQL.getColumns().toArray(new String[0]));
        conf.setStrings("operationColumns", parsedSQL.getOperationColumns().toArray(new String[0]));

        // creating job and defining jar
        Job job = Job.getInstance(conf, "GroupBy");
        job.setJarByClass(GroupBy.class);

        // setting combiner class
        job.setCombinerClass(GroupByCombiner.class);

        // setting the reducer
        job.setReducerClass(GroupByReducer.class);

        // defining output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // passing the required csv file as file path
        MultipleInputs.addInputPath(job,
                new Path("/" + DBManager.getFileName(parsedSQL.getTable1())),
                TextInputFormat.class, GroupByMapper.class);

        // defining path of output file
        Path outputPath = new Path("/output"); // hardcoded for now
        FileOutputFormat.setOutputPath(job, outputPath);

        // deleting existing outputPath file to allow reusability
        outputPath.getFileSystem(conf).delete(outputPath, true);

        long startTime = Time.now();
        long endTime = (job.waitForCompletion(true) ? Time.now() : startTime);
        long execTime = endTime - startTime;

        // writing time of execution as output
        groupByOutput.setHadoopExecutionTime(execTime + " milliseconds");

        // creating scheme for mapper
        StringBuilder mapperScheme = new StringBuilder("<serial_number, (");

        // mapper input value
        appendColumns(parsedSQL.getColumns(), mapperScheme);
        mapperScheme.append(")> ---> <(");

        // mapper output key
        appendColumns(parsedSQL.getColumns(), mapperScheme);
        mapperScheme.append("), ");

        String aggCol = null;

        // mapper output value
        switch (parsedSQL.getAggregateFunction()) {
            case COUNT:
                mapperScheme.append("1");
                break;
            case SUM:
            case MAX:
            case MIN:
                aggCol = parsedSQL.getColumns()
                        .get(parsedSQL.getColumns().size() - 1)
                        .split("\\(")[1]
                        .split("\\)")[0];
                mapperScheme.append(aggCol);
            default:
                // not likely to be encountered
                throw new IllegalArgumentException("The aggregate function is not valid");
        }

        // close mapper output
        mapperScheme.append(">");

        // write mapper scheme
        groupByOutput.setGroupByMapperPlan(mapperScheme.toString());

        // creating reducer scheme
        StringBuilder reducerScheme = new StringBuilder("<(");

        // reducer input key
        appendColumns(parsedSQL.getColumns(), reducerScheme);
        reducerScheme.append("), {");

        // reducer input value
        switch (parsedSQL.getAggregateFunction()) {
            case COUNT:
                reducerScheme.append("1, 1, 1, ... 1");
                break;
            case SUM:
            case MIN:
            case MAX:
                reducerScheme.append(aggCol).append("(1), ")
                        .append(aggCol).append("(2), ... ")
                        .append(aggCol).append("(n)");
        }

        // reducer input ends, output starts
        reducerScheme.append("}> ---> <(");

        // reducer output key
        appendColumns(parsedSQL.getColumns(), reducerScheme);
        reducerScheme.append("), ");

        // reducer output value
        reducerScheme.append(parsedSQL.getColumns().get(parsedSQL.getColumns().size() - 1)).append(">");

        // setting reducer plan
        groupByOutput.setGroupByReducerPlan(reducerScheme.toString());

        // setting hadoop output URL
        groupByOutput.setHadoopOutputUrl(
                "http://localhost:9870/webhdfs/v1/output/part-r-00000?op=OPEN " +
                        " (Note: WebHDFS should be enabled for this to work)"
        );

        return groupByOutput;
    }

    /**
     * Method which appends all columns to a scheme
     *
     * @param columns The columns to be appended
     * @param scheme  The scheme of the map/reduce job to which the columns should be appended
     */
    private static void appendColumns(ArrayList<String> columns, StringBuilder scheme) {
        for (int i = 0; i < columns.size() - 2; i++) {
            scheme.append(columns.get(i)).append(", ");
        }
        scheme.append(columns.get(columns.size() - 2));
    }

    /**
     * Class for running the Map job for evaluating the Group By SQL Query
     */
    public static class GroupByMapper extends Mapper<Object, Text, Text, Text> {

        private static String[] columns;
        private static AggregateFunction aggregateFunction;
        private static Tables table;
        private static int comparisonNumber;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            columns = conf.getStrings("columns");
            aggregateFunction = conf.getEnum("aggregateFunction", AggregateFunction.NONE);
            table = conf.getEnum("table", Tables.NONE);
            comparisonNumber = conf.getInt("comparisonNumber", Integer.MIN_VALUE);
            super.setup(context);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            StringBuilder builder = new StringBuilder(
                    record[DBManager.getColumnIndex(table, columns[0])]);
            for (int i = 1; i < columns.length - 1; i++) {
                builder.append(",").append(
                        record[DBManager.getColumnIndex(table, columns[i])]);
            }
            // assuming group by is done on all columns
            Text keyOut = new Text(builder.toString());
            String aggregateColumn = columns[columns.length - 1]
                    .split("\\(")[1]
                    .split("\\)")[0];
            int outputValue = Integer.parseInt(record[DBManager.getColumnIndex(table,
                    aggregateColumn)]);
            switch (aggregateFunction) {
                case MAX:
                case MIN:
                    // same behavior for both
                    if (outputValue > comparisonNumber) {
                        context.write(keyOut, new Text(Integer.toString(outputValue)));
                    }
                    break;
                case SUM:
                    context.write(keyOut, new Text(Integer.toString(outputValue)));
                    break;
                case COUNT:
                    context.write(keyOut, new Text("1"));
                    break;
                default:
                    // not likely to be encountered
                    throw new IllegalArgumentException("The aggregate function is not valid");
            }
        }
    }

    /**
     * Class for running a Combiner job on the results of the Map job for the Group By SQL query
     */
    public static class GroupByCombiner extends Reducer<Text, Text, Text, Text> {

        private static AggregateFunction aggregateFunction;
        private static int comparisonNumber;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            aggregateFunction = conf.getEnum("aggregateFunction", AggregateFunction.NONE);
            comparisonNumber = conf.getInt("comparisonNumber", Integer.MIN_VALUE);
            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            switch (aggregateFunction) {
                case MIN:
                    int min = Integer.MAX_VALUE;
                    while (it.hasNext()) {
                        min = Math.min(Integer.parseInt(it.next().toString()), min);
                    }
                    if (min > comparisonNumber) {
                        context.write(key, new Text(Integer.toString(min)));
                    }
                    break;
                case MAX:
                    int max = Integer.MIN_VALUE;
                    while (it.hasNext()) {
                        max = Math.max(Integer.parseInt(it.next().toString()), max);
                    }
                    if (max > comparisonNumber) {
                        context.write(key, new Text(Integer.toString(max)));
                    }
                    break;
                case SUM:
                case COUNT:
                    // both have same behavior, except count will take sum of 0s and 1s
                    long sum = 0;
                    while (it.hasNext()) {
                        sum += Integer.parseInt(it.next().toString());
                    }
                    context.write(key, new Text(Long.toString(sum)));
                    break;
                default:
                    // not likely to be encountered
                    throw new IllegalArgumentException("The aggregate function is not valid");
            }
        }
    }

    /**
     * Class for running a Reducer job on the results of the Combiner for the Group By SQL query
     */
    public static class GroupByReducer extends Reducer<Text, Text, Text, Text> {

        private static AggregateFunction aggregateFunction;
        private static int comparisonNumber;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            aggregateFunction = conf.getEnum("aggregateFunction", AggregateFunction.NONE);
            comparisonNumber = conf.getInt("comparisonNumber", Integer.MIN_VALUE);
            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            switch (aggregateFunction) {
                case MIN:
                    int min = Integer.MAX_VALUE;
                    while (it.hasNext()) {
                        min = Math.min(Integer.parseInt(it.next().toString()), min);
                    }
                    if (min > comparisonNumber) {
                        context.write(key, new Text("," + min));
                    }
                    break;
                case MAX:
                    int max = Integer.MIN_VALUE;
                    while (it.hasNext()) {
                        max = Math.max(Integer.parseInt(it.next().toString()), max);
                    }
                    if (max > comparisonNumber) {
                        context.write(key, new Text("," + max));
                    }
                    break;
                case SUM:
                case COUNT:
                    // both have same behavior, except count will take sum of 0s and 1s
                    long sum = 0;
                    while (it.hasNext()) {
                        sum += Integer.parseInt(it.next().toString());
                    }
                    if (sum > comparisonNumber) {
                        context.write(key, new Text("," + sum));
                    }
                    break;
                default:
                    // not likely to be encountered
                    throw new IllegalArgumentException("The aggregate function is not valid");
            }
        }
    }
}
