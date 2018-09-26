import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MaxTemperatureReduceer extends Reducer <Text, IntWritable,Text,IntWritable>{
    public void reduce(Text key,Iterable<IntWritable> values,Context context)
            throws IOException,InterruptedException
    {
        int maxTemperature=0;
        for (IntWritable value:values)
            maxTemperature=Math.max(maxTemperature,value.get());
        context.write(key,new IntWritable(maxTemperature));
    }

}
