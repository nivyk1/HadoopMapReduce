import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;
import java.time.LocalDateTime;

public class StepsRunner {
    public static void main(String[] args) throws Exception {
        String output = "s3://nivemr/output/";
        String minPmi = args[1];
        String relMinPmi = args[2];
        String input=args[3];
        String time = args[4];
        String output1 = output + "FirstStepOutput" + time + "/";
        Configuration conf1 = new Configuration();

        conf1.set("fs.s3a.access.key", "AKIA3FLDYNYIGCCGAHPZ");
        conf1.set("fs.s3a.secret.key", "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");
        conf1.set("fs.s3a.endpoint", "s3.amazonaws.com");
        conf1.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf1.set("13", "niv");
        System.out.println("Configuring Step 1");
        Job job1 = Job.getInstance(conf1, "FirstStep");
        MultipleInputs.addInputPath(job1, new Path(new URI(input)), SequenceFileInputFormat.class,
                FirstStep.MapperClass.class);
        System.out.println("File chached");
        job1.setJarByClass(FirstStep.class);
        job1.setMapperClass(FirstStep.MapperClass.class);
        job1.setPartitionerClass(FirstStep.PartitionerClass.class);
        //job.setCombinerClass(FirstStep.Combiner.class);
        job1.setReducerClass(FirstStep.ReducerClass.class);
        job1.setNumReduceTasks(33);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(output1));
        System.out.println("Launching Step 1");
        if (job1.waitForCompletion(true)) {
            System.out.println("Step 1 finished");
            System.out.println(job1.getConfiguration().get(conf1.get(String.valueOf(13))));
            System.out.println (job1.getConfiguration().get("changed"));
        } else {
            System.out.println("Step 1 failed ");
        }


        time = LocalDateTime.now().toString().replace(':', '-');
        System.out.println();
        String output2 = output + "SecondStepOutput" + time + "/";
        System.out.println("output2 = " + output2);
        Configuration conf2 = new Configuration();

        conf2.set("fs.s3a.access.key", "AKIA3FLDYNYIGCCGAHPZ");
        conf2.set("fs.s3a.secret.key", "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");
        conf2.set("fs.s3a.endpoint", "s3.amazonaws.com");
        conf2.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        Configuration conf3 = new Configuration();
        CounterGroup jobCounters;

        jobCounters = job1.getCounters().getGroup("DCounter");
        for (Counter counter : jobCounters){
            System.out.println("Passing " + counter.getName() + " with value " + counter.getValue() + " to step 2");
            conf2.set(counter.getName(), String.valueOf(counter.getValue()) );
            conf3.set(counter.getName(), String.valueOf(counter.getValue()) );
        }

        System.out.println("Configuring Step 2");
        Job job2 = Job.getInstance(conf2, "Step2");
        job2.setJarByClass(SecondStep.class);
        job2.setMapperClass(SecondStep.MapperClass.class);
        job2.setPartitionerClass(SecondStep.PartitionerClass.class);
        job2.setReducerClass(SecondStep.ReducerClass.class);

        //job2.setCombinerClass(SecondStep.Combiner.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(33);
        FileInputFormat.setInputPaths(job2, new Path(output1));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        System.out.println("Launching Step 2");
        if (job2.waitForCompletion(true)) {
            System.out.println("Step 2 finished");
        } else {
            System.out.println("Step 2 failed ");
        }




        System.out.println();
        String output3 = output + "Step3Output" + time + "/";
        System.out.println("Configuring Step 3");

        System.out.println(output3);
        conf3.set("fs.s3a.access.key", "AKIA3FLDYNYIGCCGAHPZ");
        conf3.set("fs.s3a.secret.key", "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");
        conf3.set("fs.s3a.endpoint", "s3.amazonaws.com");
        conf3.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        conf3.set("minPmi",minPmi);
        conf3.set("relMinPmi",relMinPmi);
        

        Job job3 = Job.getInstance(conf3, "Step3");
        job3.setJarByClass(ThirdStep.class);
        job3.setMapperClass(ThirdStep.MapperClass.class);
        job3.setReducerClass(ThirdStep.ReducerClass.class);
        job3.setPartitionerClass(ThirdStep.PartitionerClass.class);

        //no need for combiner in 3rd step (every key is a unique bigram with year and logLambda value)
        //job3.setCombinerClass(Step3.Combiner.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setNumReduceTasks(33);
        job3.setMapOutputValueClass(Text.class);
        job3.setSortComparatorClass(CustomTextComparator.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(output2));
        FileOutputFormat.setOutputPath(job3, new Path(output3));
        System.out.println("Launching Step 3");
        job3.waitForCompletion(true);
        System.out.println("All steps are done");
    }
}





