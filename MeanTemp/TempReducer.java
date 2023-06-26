import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TempReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        long count = 0;

        for (LongWritable value : values) {
            sum += value.get();
            count++;
        }

        long average = sum / count;
        context.write(key, new LongWritable(average));
    }
}
