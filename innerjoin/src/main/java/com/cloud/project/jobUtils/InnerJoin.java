package com.cloud.project.jobUtils;

import com.cloud.project.contracts.DBManager;
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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class InnerJoin {

    private static void globalMapper(Tables table, int tableKeyIndex, Text value,
                                     Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        String record = value.toString();
        String[] parts = record.split(",");
        String jk = parts[tableKeyIndex];
        StringBuilder val = new StringBuilder(table.name() + "#");
        /*
         FIXME: To fix extra ',' in output
          */
        for (int i = 0; i < parts.length; i++) {
            if (i == tableKeyIndex) continue;
            val.append(",").append(parts[i]);
        }
        context.write(new Text(jk), new Text(val.toString()));
    }

    public static void execute(ParseSQL parsedSQL) throws IOException,
            InterruptedException, ClassNotFoundException, SQLException {

        /* get key index for both tables */
        String jk = DBManager.getJoinKey(parsedSQL.getTable1(), parsedSQL.getTable2());
        if (jk == null) {
            System.out.println("No join key exists!");
            System.exit(0);
        }

        /* #########################################################################*/
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.setEnum("table1", parsedSQL.getTable1()); //args[3]);
        conf.setEnum("table2", parsedSQL.getTable2());
        conf.set("jk", jk);
        Job job = Job.getInstance(conf, "InnerJoin");
        job.setJarByClass(InnerJoin.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("/" + DBManager.getFileName(parsedSQL.getTable1())), TextInputFormat.class, firstMapper.class);
        MultipleInputs.addInputPath(job, new Path("/" + DBManager.getFileName(parsedSQL.getTable2())), TextInputFormat.class, secondMapper.class);
        Path outputPath = new Path("/output");

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }

    private static class firstMapper extends Mapper<Object, Text, Text, Text>
    {
        private static Tables table;
        private static int tableKeyIndex;

        @Override
        protected void setup(Context context) {
            table = context.getConfiguration().getEnum("table1", Tables.NONE);
            String jk = context.getConfiguration().get("jk");
            tableKeyIndex = DBManager.getColumnIndex(table, jk);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            globalMapper(table, tableKeyIndex, value, context);
        }
    }

    private static class secondMapper extends Mapper<Object, Text, Text, Text>
    {
        private static Tables table;
        private static int tableKeyIndex;

        @Override
        protected void setup(Context context) {
            table = context.getConfiguration().getEnum("table2", Tables.NONE);
            String jk = context.getConfiguration().get("jk");
            tableKeyIndex = DBManager.getColumnIndex(table, jk);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            globalMapper(table, tableKeyIndex, value, context);
        }
    }

    private static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>
    {
        private static Tables table1;
        private static Tables table2;

        @Override
        protected void setup(Reducer.Context context) {
            table1 = context.getConfiguration().getEnum("table1", Tables.NONE);
            table2 = context.getConfiguration().getEnum("table2", Tables.NONE);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> table1List = new ArrayList<>();
            ArrayList<String> table2List = new ArrayList<>();
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

            for (String u : table1List) {
                for (String z : table2List) {
                    context.write(key, new Text(u + ',' + z));
                }

            }
        }
    }
}
