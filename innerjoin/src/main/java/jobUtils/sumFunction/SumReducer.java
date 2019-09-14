package jobUtils.sumFunction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Object, Text, Text, Text> {
}
