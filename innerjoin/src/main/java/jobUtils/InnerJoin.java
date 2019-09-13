package jobUtils;

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
import java.util.ArrayList;
import java.util.List;

public class InnerJoin {
    public static class userMapper extends Mapper <Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[4]),
                    new Text("user#" + ","+parts[0]+","+parts[1]+","+parts[2]+","+parts[3]));
        }
    }

    public static class zipMapper extends Mapper <Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("zip#"+parts[1]+","+parts[2]+","+parts[3]));
        }
    }

    public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            List users = new ArrayList<>();
            List zips = new ArrayList<>();
            //String name = "";
            //double total = 0.0;
            //int count = 0;
            for (Text t : values) {
                String parts[] = t.toString().split("#");
                if (parts[0].equals("user")) {
                    users.add(parts[1]);
                }
                else if (parts[0].equals("zip")) {
                    zips.add(parts[1]);
                }
            }

            for (Object u : users) {
                for (Object z: zips) {
                    context.write(key, new Text(u.toString()+','+z.toString()));
                }

            }
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJobName("InnerJoin");
        job.setJarByClass(InnerJoin.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, userMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, zipMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
