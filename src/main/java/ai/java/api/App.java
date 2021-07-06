/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ai.java.api;

import KMeans.Centroid;
import KMeans.EuclideanDistance;
import KMeans.KMean;
import KMeans.SampleData;
import Main.EDA;
import Main.Job;
import Main.JobDAOImpl;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.spark.ml.linalg.Vector;
import Main.JobVisualization;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author mersahl
 */
@Path("/")
public class App {

    private static List<Job> jobs;
    private static SparkSession sparkSession;
    private static EDA eda;

    private static final String JOB_DATASET_PATH
            = "./src/main/webapp/WEB-INF/Wuzzuf_Jobs.csv";
//    FIXME
//    private static final String jobDatasetPath = "/media/mersahl/hdd/home/mersahl/workspace/iti-ai-pro/java-uml/WebApplication0/src/main/webapp/WEB-INF/Wuzzuf_Jobs.csv";

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/all-jobs")
    public Response allJobs() {
        init();
        GenericEntity<List<Job>> entity = new GenericEntity<List<Job>>(jobs) {
        };
//        TODO implement api
        kill();
        return Response.ok(entity).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/jobs/{method}")
    public Response summary(@PathParam("method") String method) {
        init();
        List<Row> jobsRow = null;
        switch (method) {
            case "showsummery":
                jobsRow = eda.GetSummeryDataset();
                GenericEntity<List<Row>> geSummary = new GenericEntity<List<Row>>(jobsRow) {
                };
                kill();
                return Response.ok(geSummary).build();
            case "showdescribe":
                jobsRow = eda.GetDatasetDescribe();
                GenericEntity<List<Row>> geDesc = new GenericEntity<List<Row>>(jobsRow) {
                };
                kill();
                return Response.ok(geDesc).build();
            case "showdataset":
                jobs = Preprocessing(jobs);
                GenericEntity<List<Job>> geDataset = new GenericEntity<List<Job>>(jobs) {
                };
                kill();
                return Response.ok(geDataset).build();
        }
        kill();
        return Response.status(400).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/job/{subquery}")
    public Response jobAPI(@PathParam("subquery") String subquery) {
        init();
        jobs = Preprocessing(jobs);

        Map<String, Long> valuesMap;
        GenericEntity<Map<String, Long>> entity = null;
        switch (subquery) {
            case "showcompanyjobscount":
                valuesMap = eda.GetSortedCompanyJobsCount(jobs);
                JobVisualization.PieChartCompanyJobsCount(valuesMap);
                entity = new GenericEntity<Map<String, Long>>(valuesMap) {
                };
                break;
            case "showjobstitlecount":
                valuesMap = eda.GetSortedJobsTitleCount(jobs);
                JobVisualization.BarChartJobsCount(valuesMap);
                entity = new GenericEntity<Map<String, Long>>(valuesMap) {
                };
                break;
            case "showareacount":
                valuesMap = eda.GetSortedAreaCount(jobs);
                JobVisualization.BarChartAreaJobsCount(valuesMap);
                entity = new GenericEntity<Map<String, Long>>(valuesMap) {
                };
                break;
            case "showskillscount":
                valuesMap = eda.GetSortedSkillsCount(jobs);
                JobVisualization.PieChartSkillsCount(valuesMap);
                entity = new GenericEntity<Map<String, Long>>(valuesMap) {
                };
                break;
        }
        if(entity == null)
            return Response.status(400).build();
        return Response.ok(entity).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/jobencodedr")
    public Response jobsEncoder() {
        init();
        GenericEntity<List<Row>> entity = new GenericEntity<List<Row>>(Encoder(jobs).collectAsList()) {
        };
        kill();
        return Response.ok(entity).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/stats/{subquery}")
    public Response Kmean(@PathParam("subquery") String subquery) {
        init();
        switch (subquery)
        {
            case "scratch":
                Set<Centroid> centroids = Kmeans(jobs);
                GenericEntity<Set<Centroid>> scratchEntity = new GenericEntity<Set<Centroid>>(centroids) {
                };
                kill();
                return Response.ok(scratchEntity).build();
            case "spark":
                List<Vector> centers = SparkKmeans(jobs);
                GenericEntity<List<Vector>> sparkEntity = new GenericEntity<List<Vector>>(centers) {
                };
                kill();
                return Response.ok(sparkEntity).build();
        }
        return Response.status(400).build();
    }

    private static void init() {
        if (jobs == null) {
            jobs = new JobDAOImpl().ReadCSVFile(JOB_DATASET_PATH);
        }

        if (sparkSession == null) {
            sparkSession = SparkSession.builder().appName("jobs").master("local[*]").getOrCreate();
        }

        if (eda == null) {
            eda = new EDA(sparkSession, jobs);
        }

    }

    private static List<Job> Preprocessing(List<Job> jobs) {
        eda.RemoveDuplicate();
        eda.DropNullValue();
        return JobDAOImpl.ConvertRowDatasetToList(eda.getRowDataset());
    }

    private static Dataset<Row> Encoder(List<Job> jobs) {
        jobs = Preprocessing(jobs);
        Dataset<Row> jobDataset = eda.ConvertJobsToDatasetRow(jobs);
        String[] ColumnsName = {"Title", "Company"};
        return HotEncoder.OneHotEncoding.GetOneHotEncoding(sparkSession, EDA.getRowDataset(jobDataset, ColumnsName),
                ColumnsName);
    }

    private static Set<Centroid> Kmeans(List<Job> jobs) {
        Dataset<Row> encoded = Encoder(jobs);
        String[] EncodedColumnsName = {"TitleIndex", "CompanyIndex"};
        encoded = EDA.getRowDataset(encoded, EncodedColumnsName);
        List<SampleData> samples = SampleData.GetSamplesListFromRowList(encoded);
        Map<Centroid, List<SampleData>> clusters = KMean.fit(samples, 10, new EuclideanDistance(), 1000);
        return clusters.keySet();
    }

    private static List<Vector> SparkKmeans(List<Job> jobs) {
        Dataset<Row> encoded = Encoder(jobs);
        String[] EncodedColumnsName = {"TitleIndex", "CompanyIndex"};
        encoded = EDA.getRowDataset(encoded, EncodedColumnsName);
        return KMean.SparkKmeans(sparkSession, encoded, 10, 1000);
    }

    private static void kill() {
        sparkSession.stop();
    }
}