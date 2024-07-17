import com.amazonaws.auth.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
//import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;

public class LocalApp {
    private static String accessKey;
    private static String secretKey;

    public static void loadCredentials(String path){
        try {
            System.out.println("first");
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            accessKey = line.split("=", 2)[1];
            line = reader.readLine();
            secretKey = line.split("=", 2)[1];
            reader.close();
        }
        catch (IOException e)
        {
            System.out.println(e);
        }

    }
    public static void main(String[] args) {




        loadCredentials(System.getProperty("user.home") + File.separator + ".aws" + File.separator + "credentials.txt");

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);


          String  hebrewInput = "s3a://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";

        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        //LocalDateTime now = LocalDateTime.now();
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://nivemr/jar/StepsRunner.jar") // This should be a full map reduce application.
                .withMainClass("StepsRunner")
                .withArgs(args[0],args[1],hebrewInput, LocalDateTime.now().toString().replace(':', '-'));
        StepConfig stepConfig = new StepConfig()
                .withName("Steps")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(9)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion("2.9.2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Google Bigrams collocation extract")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3://nivemr/logs/")
                .withReleaseLabel("emr-5.30.0");


        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("EMR Job ID: " + jobFlowId);
    }
}