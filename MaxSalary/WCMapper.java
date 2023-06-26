import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text name = new Text();
    private IntWritable salary = new IntWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens = line.split(" ");

        if (tokens.length == 2) {
            name.set(tokens[0]);
            int salaryValue = Integer.parseInt(tokens[1]);
            salary.set(salaryValue);
            context.write(name, salary);
        }
    }
}
