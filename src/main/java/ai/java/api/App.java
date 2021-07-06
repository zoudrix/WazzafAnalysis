/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ai.java.api;

import Main.EDA;
import Main.Job;
import Main.JobDAOImpl;
import java.net.URL;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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

//    private static final String JOB_DATASET_PATH
    //            = "./src/main/webapp/WEB-INF/Wuzzuf_Jobs.csv";
//            = "./src/main/resources/Datasets/Wuzzuf_Jobs.csv";
//    FIXME
    private static final String JOB_DATASET_PATH
            = "/media/mersahl/hdd/home/mersahl/workspace/iti-ai-pro/java-uml/WebApplication0/src/main/webapp/WEB-INF/Wuzzuf_Jobs.csv";

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
        EDA eda = new EDA(sparkSession, jobs);
        GenericEntity<List<Job>> entity = new GenericEntity<List<Job>>(jobs) {
        };
        switch (method) {
            case "showsummery":
                List<Row> rows = eda.GetSummeryDataset();
                GenericEntity<List<Row>> e = new GenericEntity<List<Row>>(rows) {
                };
                return Response.ok(e).build();
            case "showdescribe":
                eda.ShowDatasetDescribe();
                break;
            case "showdataset":
                eda.RemoveDuplicate();
                eda.DropNullValue();
                jobs = JobDAOImpl.ConvertRowDatasetToList(eda.getRowDataset());
                break;
            default:
                eda.ShowDataset();
        }
        kill();

        return null;
    }

    @GET
    @Produces("text/plain")
    @Path("/job/{subquery}")
    public Response jobAPI(@PathParam("subquery") String subquery) {
        init();
//        TODO implement api
        kill();
        return null;
    }

    public static void main(String[] args) {
        jobs = new JobDAOImpl().ReadCSVFile(JOB_DATASET_PATH);
    }
    
    private static void init() {
        if (jobs == null) {
            jobs = new JobDAOImpl().ReadCSVFile(JOB_DATASET_PATH);
        }
        if (sparkSession == null) {
            sparkSession
                    = SparkSession.builder().appName("jobs").master("local[*]").getOrCreate();
        }
    }

    private static void kill() {
        sparkSession.stop();
    }
//    TODO Skills
//    TODO encoder
//    TODO KMeans
}
