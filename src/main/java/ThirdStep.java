
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
                //w1,w2,decade,cw1,cw2	cw1w2
                System.out.println("entering map of step3");
                String[] tmp = value.toString().split("\t");
                String[] prevKey = tmp[0].split(",");
                String w1 = prevKey[0];
                String w2 = prevKey[1];
                String decade = prevKey[2];
                Double cw1 = Double.valueOf(prevKey[3]);
                Double cw2 = Double.valueOf(prevKey[4]);
                Double cw1w2 = Double.valueOf(tmp[1]);
                System.out.println("cw1w2:"+cw1w2);
                String NcounterName = "D_" + decade;
                String s= context.getConfiguration().get(NcounterName);
                double N=Double.parseDouble(context.getConfiguration().get(NcounterName));
               // double N=Double.valueOf(context.getCounter("DCounter",NcounterName).getValue());



                double pmi= Math.log(cw1w2)+Math.log(N)-Math.log(cw1)- Math.log(cw2);
               double pw1w2=cw1w2/N;
                double npmi=pmi/(-(Math.log(pw1w2)));
                long saclednpmi = (long) (npmi * 1000000000);
                context.getCounter("NMPISUM", "S_" + decade).increment(saclednpmi);

                if(npmi>0 && npmi!=1 && pw1w2!=1 ) {
                    //In order to get Desc order we defined a new CustomTextComparator
                    //emited key:"npmi w1 w2 decade"

                    context.write(new Text(npmi + " " + w1 + " " + w2 + " " + decade), new Text(String.valueOf(cw1w2)));
                }


            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("exiting map of step3");
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

            //emited key:"npmi w1 w2 decade"
            System.out.println("entering reduce of step3");
            String []tmp =key.toString().split(" ");
            double npmi= (Double.parseDouble(tmp[0]));

            //System.out.println("reducer npmi:"+npmi);
            String w1= tmp[1];
            String w2= tmp[2];
            int decade= Integer.parseInt(tmp[3])*10;
           // context.getCounter("NMPISUM", "S_"+decade).increment(saclednpmi);

          //
            double counter=((double)(context.getCounter("NMPISUM", "S_"+decade).getValue()))/1000000000;

            if(npmi/counter>=relMinPmi || npmi>=minPmi  ){
                System.out.println("entered reduce writing section");
                context.write(new Text(String.valueOf(decade)+" "+w1+" "+w2),new Text(String.valueOf(npmi)));
            }

            System.out.println("exiting reduce of step3");
            }
        }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] decades = {"153", "154", "156", "162", "167", "168","169", "170", "175", "176",
                    "178", "179", "180", "181", "182", "183", "184", "185", "186", "187", "188",
                    "189", "190", "191", "192", "193", "194", "195", "196", "197", "198", "199", "200"};
            String current_year = key.toString().split(" ")[3];
            for(int i=0; i<decades.length; i++)
                if(decades[i].equals(current_year))
                    return (i % numPartitions);
            return 0;
        }
    }

    }



