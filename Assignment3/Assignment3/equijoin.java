import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/*
name: rkaur19
DDS Assignemnt 3
Fall 2020
 */
public class equijoin {

    public static class Mapping extends Mapper<Text, Text, Text, Text>
    {
        Text column = new Text();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if(key.toString().split(",").length >1)
            {
                column.set(key.toString().split(",")[1]);
                context.write(column, key);
            }
        }
    }

    public static class Reducing extends Reducer<Text, Text, Text, Text>
    {
        Text output = new Text();
        ArrayList<String> arrayList_1;
        ArrayList<String> arrayList_2;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            arrayList_1 = new ArrayList<String>();
            arrayList_2 = new ArrayList<String>();
            for (Text line : values)
            {
                String table = line.toString().split(",")[0];
                if(arrayList_1.size()==0)
                    arrayList_1.add(line.toString());
                else
                {
                    if(arrayList_1.get(0).split(",")[0].equals(table))
                        arrayList_1.add(line.toString());
                    else
                        arrayList_2.add(line.toString());
                }
            }
            if(arrayList_1.size() > 0 && arrayList_2.size() >0)
            {
                for(int i=0;i<arrayList_1.size();i++) {
                    for (int j = 0; j < arrayList_2.size(); j++) {
                        output.set(arrayList_1.get(i) + "," + arrayList_2.get(j));
                        context.write(null, output);
                        output.clear();
                    }
                }
            }
        }
    }





    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "equijoin");
        //setting object class
        job.setJarByClass(equijoin.class);

        //setting mapers
        job.setMapperClass(Mapping.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //setting reducer
        job.setReducerClass(Reducing.class);

        //setting data format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[args.length-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

        job.waitForCompletion(true);
    }
}