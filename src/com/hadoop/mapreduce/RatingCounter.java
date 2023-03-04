import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RatingCounter extends Configured implements Tool {

    private static final IntWritable ONE = new IntWritable(1); //hadoop does not accept normal int

    public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        @Override
        // below is the function to get data and map it to key pair value
        public void map(Object key, Text value, Context context)
         throws IOException, InterruptedException {
            word.set(value.toString().split("::")[1]); // to take each parameter from a line of data
            context.write(word, ONE); //write to the file
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int counter = StreamSupport.stream(values.spliterator(), true).mapToInt(IntWritable::get).sum();
            // get total of all value groupby key
            context.write(key, new IntWritable(counter));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Counter");
        job.setJarByClass(RatingCounter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new RatingCounter(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}