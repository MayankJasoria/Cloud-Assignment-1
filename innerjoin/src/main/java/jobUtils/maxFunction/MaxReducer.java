package jobUtils.maxFunction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxReducer extends Reducer<Object, Text, Text, Text> {
}
