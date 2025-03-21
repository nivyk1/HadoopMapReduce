import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondStep {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {

                //tmp[0] is the key emitted from step1 reducer, tmp[1] is the value emitted from step1 reducer
                String[] tmp = value.toString().split("\t");
                String[] oldKey = tmp[0].split(",");	// w1 w2 year
               String[] oldval= tmp[1].split("@");// c(w1,w2)@c(w1)
                String cw1w2 = oldval[0];
                String cw1 = oldval[1];
                context.write(new Text(oldKey[1] + "," +  "*" + "," + oldKey[2]), new Text(cw1w2));//key:  w2,* val: cw1w2
                context.write(new Text(oldKey[1] + "," + oldKey[0] + "," + oldKey[2] + "," + cw1), new Text(cw1w2));//key: w2,w1,year,cw1 val: cw1w2

                // change order of w1w2 to w2w1 so now we will sort it in lexicographic order of w2, so we can find c(w2)

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            try {
                for (Text txt : values) {
                    sum += Integer.parseInt(txt.toString());
                }
                context.write(key, new Text(String.valueOf(sum)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        int cw2;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            try {
                if (key.toString().contains("*")) {
                    cw2 = 0;
                    for (Text val : values)
                        cw2 = cw2 + Integer.parseInt(val.toString());
                } else {
                    for (Text val : values)
                        sum = sum + Integer.parseInt(val.toString());
                    String[] tmp = key.toString().split(",");
                    context.write(new Text(tmp[1] + "," + tmp[0] + "," + tmp[2] + "," + tmp[3] + "," + cw2), new Text(String.valueOf(sum)));
                    //w1,w2,decade,cw1,cw2	cw1w2
                     }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
