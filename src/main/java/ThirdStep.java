
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class ThirdStep {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        //w1,w2,year,cw1,cw2	cw1w2
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] tmp = value.toString().split("\t");
                String[] prevKey = tmp[0].split(",");
                String w1 = prevKey[0];
                String w2 = prevKey[1];
                String decade = prevKey[2];
                Double cw1 = Double.valueOf(prevKey[3]);
                Double cw2 = Double.valueOf(prevKey[4]);
                Double cw1w2 = Double.valueOf(tmp[1]);
                String NcounterName = "N_" + decade;
                double N=Double.valueOf(context.getCounter("DCounter",NcounterName).getValue());


                double pmi= Math.log(cw1w2)+Math.log(N)-Math.log(cw1)- Math.log(cw2);
                double pw1w2=cw1w2/N;
                double npmi=pmi/(-(Math.log(pw1w2)));

               long saclednpmi=(long) (npmi*1000000000);

               context.getCounter("NMPISUM", "S_"+decade).increment(saclednpmi);




                //multiply by -1 so Hadoop sorts it and in reduce() we multiply by -1 again to get the npmi in DESC order
                //emited key:"-npmi w1 w2 decade"
                context.write(new Text(((-1) * npmi) + "," + w1 + "," + w2 + "," + decade), new Text(String.valueOf(cw1w2)));


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        double minPmi;
        double relMinPmi;
        @Override

        protected void setup(Context context) throws IOException, InterruptedException {
            minPmi=  Double.parseDouble(context.getConfiguration().get("minPmi"));
          relMinPmi=  Double.parseDouble(context.getConfiguration().get("relMinPmi"));
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //emited key:"-npmi w1 w2 decade"

            String []tmp =key.toString().split(" ");
            double npmi= Double.parseDouble(tmp[0])*(-1);
            String w1= tmp[1];
            String w2= tmp[2];
            int decade= Integer.parseInt(tmp[3])*10;
           // context.getCounter("NMPISUM", "S_"+decade).increment(saclednpmi);

          //
            double counter=((double)(context.getCounter("NMPISUM", "S_"+decade).getValue()))/1000000000;

            if(npmi/counter>=relMinPmi || npmi>=minPmi) {
                context.write(new Text(String.valueOf(decade)+" "+w1+" "+w2),new Text(String.valueOf(npmi)));
            }


            }
        }
    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String decade = key.toString().split(",")[3];

            // check all decades from 1500 to 2020[150 to 202)
            for(int i=0; i<71; i++) {
                if (Integer.toString(i + 150).equals(decade))
                    return (i % numPartitions);
            }
            return 0;
        }
    }




    }



