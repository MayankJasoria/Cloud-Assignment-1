package jobUtils.countFunction;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<Object, Text, Text, Text> {
}
