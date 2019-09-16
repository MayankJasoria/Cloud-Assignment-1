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
import sqlUtils.ParseSQL;
import sqlUtils.Tables;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class InnerJoin {

    private static ParseSQL parseSQL;

    public static void globalMapper(Tables table, int tableKeyIndex, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String record = value.toString();
        String[] parts = record.split(",");
        String jk = parts[tableKeyIndex];
        String val = table.name() + "#";
        /*
         FIXME: To fix extra ',' in output
          */
        for (int i = 0; i < parts.length; i++) {
            if (i == tableKeyIndex) continue;
            val = val + "," + parts[i];
        }
        context.write(new Text(jk), new Text(val));
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, SQLException {

        parseSQL = null;

        /* While sqlUtils is under construction, take table names as params 3 and 4 */
        firstMapper.table = getTable(args[3]);
        secondMapper.table = getTable(args[4]);
        ReduceJoinReducer.table1 = firstMapper.table;
        ReduceJoinReducer.table2 = secondMapper.table;
        /* get key index for both tables */
        String jk = DBManager.getJoinKey(firstMapper.table, secondMapper.table);
        if (jk == null) {
            System.out.println("No join key exists!");
            System.exit(0);
        }
        firstMapper.tableKeyIndex = DBManager.getColumnIndex(firstMapper.table, jk);
        secondMapper.tableKeyIndex = DBManager.getColumnIndex(secondMapper.table, jk);

        /* #########################################################################*/
        Configuration conf = new Configuration();
        conf.set("table1", args[3]);
        conf.set("table2", args[4]);
        conf.set("jk", jk);
        Job job = Job.getInstance(conf, "InnerJoin");
        job.setJarByClass(InnerJoin.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, firstMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, secondMapper.class);
        Path outputPath = new Path(args[2]);


        FileOutputFormat.setOutputPath(job, outputPath);


        outputPath.getFileSystem(conf).delete(outputPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Temporary helper function. This won't be necessary after our main function is
     * refactored into execute().
     */

    public static Tables getTable(String table) throws SQLException {
        if (table.equalsIgnoreCase(Tables.USERS.name())) {
            return Tables.USERS;
        } else if (table.equalsIgnoreCase(Tables.ZIPCODES.name())) {
            return Tables.ZIPCODES;
        } else if (table.equalsIgnoreCase(Tables.MOVIES.name())) {
            return Tables.MOVIES;
        } else if (table.equalsIgnoreCase(Tables.RATING.name())) {
            return Tables.RATING;
        } else {
            throw new SQLException("Table " + table + " does not exist");
        }

    }

    public static class firstMapper extends Mapper<Object, Text, Text, Text>
    {
        public static Tables table;
        public static int tableKeyIndex;

        @Override
        protected void setup(Context context) {
            try {
                table = getTable(context.getConfiguration().get("table1"));
                String jk = context.getConfiguration().get("jk");
                tableKeyIndex = DBManager.getColumnIndex(table, jk);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            globalMapper(table, tableKeyIndex, value, context);
        }
    }

    public static class secondMapper extends Mapper<Object, Text, Text, Text>
    {
        public static Tables table;
        public static int tableKeyIndex;

        @Override
        protected void setup(Context context) {
            try {
                table = getTable(context.getConfiguration().get("table2"));
                String jk = context.getConfiguration().get("jk");
                tableKeyIndex = DBManager.getColumnIndex(table, jk);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            globalMapper(table, tableKeyIndex, value, context);
        }
    }

    public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
    {
        public static Tables table1;
        public static Tables table2;

        @Override
        protected void setup(Reducer.Context context) {
            try {
                table1 = getTable(context.getConfiguration().get("table1"));
                table2 = getTable(context.getConfiguration().get("table2"));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List table1List = new ArrayList<>();
            List table2List = new ArrayList<>();
            //String name = "";
            //double total = 0.0;
            //int count = 0;
            for (Text t : values) {
                String[] parts = t.toString().split("#");
                if (parts[0].equals(table1.name())) {
                    table1List.add(parts[1]);
                } else if (parts[0].equals(table2.name())) {
                    table2List.add(parts[1]);
                }
            }

            for (Object u : table1List) {
                for (Object z : table2List) {
                    context.write(key, new Text(u.toString()+','+z.toString()));
                }

            }
        }
    }
}
