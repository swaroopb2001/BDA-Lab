import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(" ");
        String industry = tokens[0];

        for (int i = 1; i < tokens.length; i++) {
            long temperature = Long.parseLong(tokens[i]);
            context.write(new Text(industry), new LongWritable(temperature));
        }
    }
}
