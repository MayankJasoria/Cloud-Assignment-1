package jobUtils;

import jobUtils.countFunction.CountMapper;
import jobUtils.countFunction.CountReducer;
import jobUtils.maxFunction.MaxMapper;
import jobUtils.maxFunction.MaxReducer;
import jobUtils.minFunction.MinMapper;
import jobUtils.minFunction.MinReducer;
import jobUtils.sumFunction.SumMapper;
import jobUtils.sumFunction.SumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import sqlUtils.ParseSQL;

import java.io.IOException;
import java.sql.SQLException;

public class GroupBy {

    private ParseSQL parseSQL;

    public GroupBy(ParseSQL parseSQL) {
        this.parseSQL = parseSQL;
    }

    public void execute() throws IOException, InterruptedException, ClassNotFoundException, SQLException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "aggregateFunction");

        // initiate aggregate job according to the required aggregate function
        switch (parseSQL.getAggregateFunction()) {
            case COUNT:
                // Jar location should be the same as the one where mapper and reducer are present
                job.setJarByClass(CountMapper.class);

                // setting mapper and reducer
                job.setMapperClass(CountMapper.class);
                job.setReducerClass(CountReducer.class);

                // setting output values
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case SUM:
                job.setJarByClass(SumMapper.class);

                // setting mapper and reducer
                job.setMapperClass(SumMapper.class);
                job.setReducerClass(SumReducer.class);

                // setting output values
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case MAX:
                job.setJarByClass(MaxMapper.class);

                // setting mapper and reducer
                job.setMapperClass(MaxMapper.class);
                job.setReducerClass(MaxReducer.class);

                // setting output values
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case MIN:
                job.setJarByClass(MinMapper.class);

                // setting mapper and reducer
                job.setMapperClass(MinMapper.class);
                job.setReducerClass(MinReducer.class);

                // setting output values
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
        }
    }
}
