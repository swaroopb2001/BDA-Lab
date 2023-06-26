import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable maxSalary = new IntWritable(Integer.MIN_VALUE);

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int currentSalary;
        for (IntWritable value : values) {
            currentSalary = value.get();
            maxSalary.set(Math.max(maxSalary.get(), currentSalary));
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Maximum Salary"), maxSalary);
    }
}
