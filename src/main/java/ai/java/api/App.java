/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ai.java.api;

import Main.Job;
import Main.JobDAOImpl;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author mersahl
 */
@Path("/")
public class App {

    private static List<Job> jobs;
    private static SparkSession sparkSession;

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
    @Produces("text/plain")
    @Path("/data/{method}")
    public Response summary(@PathParam("method") String method) {
        init();
//        TODO implement api
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

    private static void init() {
        if (jobs == null) {
            jobs = new JobDAOImpl().ReadCSVFile(JOB_DATASET_PATH);
        }
        if (sparkSession == null) {
            sparkSession = SparkSession.builder().appName("jobs").master("local[*]").getOrCreate();
        }
    }

    private static void kill() {
        sparkSession.stop();
    }
//    TODO Skills
//    TODO encoder
//    TODO KMeans
}
