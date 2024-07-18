
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class FirstStep {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        Map<String, Boolean> StopWords = new HashMap<>();
        String[] StopWordsArray = {
                "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ", "למה",
                "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה",
                "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא",
                "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה", "אל", "אך", "איש",
                "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"", "!", "שלשה",
                "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת",
                "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי",
                "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם",
                "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין",
                "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א","\"", "!", "#", "*", "+", "'", ",", "`", "/", "-", "@","=","$","%","—"
        };


        protected void setup(Context context) throws IOException, InterruptedException {

            System.out.println("entering setup");
            Configuration conf = context.getConfiguration();
         for(String s :  StopWordsArray)
         {
          StopWords.put(s,true)   ;
         }

            System.out.println("existing setup");

        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {

                System.out.println("Entering map method with key: " + key + "and value: " + value);
                boolean Stop = false;

                try {
                    Configuration c = context.getConfiguration();
                    //line Format: w1 w2 /t year /t count
                    String[] line = value.toString().split("\t");
                    //contains the 2 words of the google 2grams
                    String[] words = line[0].split(" ");

                    if (words.length == 2) {
                        String word1 = words[0];
                        String word2 = words[1];
                        String decade = String.valueOf(Integer.parseInt(line[1]) / 10);    //for example: 1987 -> 198
                        String count = line[2];
                        String decadeCounterName = "D_" + decade;


                        if (StopWords.containsKey(word1) || StopWords.containsKey(word2)) {
                            Stop = true;
                        }

                        if (!Stop) {

                            context.write(new Text(word1 + "," + "*" + "," + decade), new Text(count)); // (w1,*,decade	count)
                            context.write(new Text(word1 + "," + word2 + "," + decade), new Text(count)); // (w1,w2,decade	count)


                            context.getCounter("DCounter", decadeCounterName).increment(Integer.parseInt(count));    //inc the proper decade counter
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            }



    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        int cword1 = 0;

        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("setup of reduce() step1");
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;

            try {
                if (key.toString().contains("*")) {	//<w,*>
                    cword1 = 0;
                    for (Text val : values)
                        cword1 = cword1 + Integer.parseInt(val.toString());
                } else {							//<W1, W2>
                    for (Text val : values) {
                        sum = sum + Integer.parseInt(val.toString());
                    }

                    //each (w1,w2) emits c(w1,w2)@c(w1)
                    context.write(key, new Text(sum + "@" + cword1));

                }
            }
            catch (Exception e) {
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


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] decades = {"153", "154", "156", "162", "167", "168","169", "170", "175", "176",
                    "178", "179", "180", "181", "182", "183", "184", "185", "186", "187", "188",
                    "189", "190", "191", "192", "193", "194", "195", "196", "197", "198", "199", "200"};
            String current_year = key.toString().split(",")[2];
            for(int i=0; i<decades.length; i++)
                if(decades[i].equals(current_year))
                    return (i % numPartitions);
            return 0;
        }
        }

}
