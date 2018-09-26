import java.io.IOException;

import com.sun.tools.corba.se.idl.StringGen;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
        String line=value.toString();
        String year=line.substring(16,19);
        int temperature=0;
        if (line.charAt(87)=='+')
            temperature=Integer.parseInt(line.substring(88,92));
        else
            temperature=Integer.parseInt(line.substring(87,92));
        context.write(new Text(year),new IntWritable(temperature));
    }
}