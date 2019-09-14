package jobUtils.minFunction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinReducer extends Reducer<Object, Text, Text, Text> {
}
