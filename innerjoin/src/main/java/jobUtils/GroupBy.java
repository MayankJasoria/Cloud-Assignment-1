package jobUtils;

import contracts.DBManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sqlUtils.AggregateFunction;
import sqlUtils.ParseSQL;
import sqlUtils.Tables;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;


public class GroupBy {

    public static void execute(ParseSQL parsedSQL) throws IOException,
            InterruptedException, ClassNotFoundException, SQLException {
        Configuration conf = new Configuration();

        conf.setEnum("table", parsedSQL.getTable1());
        conf.setEnum("aggregateFunction", parsedSQL.getAggregateFunction());
        conf.setInt("comparisonNumber", parsedSQL.getComparisonNumber());
        conf.setStrings("columns", parsedSQL.getColumns().toArray(new String[0]));
        conf.setStrings("operationColumns", parsedSQL.getOperationColumns().toArray(new String[0]));

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
        MultipleInputs.addInputPath(job, new Path("/" + DBManager.getFileName(parsedSQL.getTable1())),
                TextInputFormat.class, GroupByMapper.class);

        // defining path of output file
        Path outputPath = new Path("/output"); // hardcoded for now
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(conf).delete(outputPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

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
