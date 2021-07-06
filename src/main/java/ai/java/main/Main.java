package ai.java.main;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public class Main {

    private static SparkSession sparkSession;
    private static EDA eda;
    private static final SparkSession.Builder SESSION_BUILDER
            = SparkSession.builder().appName("jobs").master("local[*]");
    private static final String JOB_DATASET_PATH
            = "./src/main/resources/Dataset/Wuzzuf_Jobs.csv";

    public static void main(String[] args) {
        sparkSession = SESSION_BUILDER.getOrCreate();
        List<Job> jobs = new JobDAOImpl().ReadCSVFile(JOB_DATASET_PATH);
        eda = new EDA(sparkSession, jobs);
        chartCompany(jobs);
        chartJob(jobs);
        chartArea(jobs);
        chartSkill(jobs);
//        Logger.getLogger("org").setLevel(Level.ERROR);
//        SparkSession sparkSession = SparkSession.builder().appName("jobs").master("local[*]").getOrCreate();
////        String jobDatasetPath = "/media/mersahl/hdd/home/mersahl/workspace/iti-ai-pro/java-uml/JavaFinalProject/src/main/resources/Dataset/Wuzzuf_Jobs.csv";
//        String jobDatasetPath = "./src/main/resources/Dataset/Wuzzuf_Jobs.csv";
//        List<Job> jobList = new JobDAOImpl().ReadCSVFile(jobDatasetPath);
//        //Dataset<Row> jobRow = new JobDAOImpl().SparkReadCSVFile(sparkSession, jobDatasetPath);
//
//        EDA eda = new EDA(sparkSession);
//        Dataset<Row> jobDataset = eda.ConvertJobsToDatasetRow(jobList);
//        System.out.prinyl(eda.GetSummeryDataset();)n
        //Row row = eda.GetSummeryDataset().get(0);
        //System.out.println("row = " + row);
//        System.out.println("---------------------------\n---------------------------\n---------------------------\n");
//        eda.ShowDataset();
//        System.out.println("---------------------------\n---------------------------\n---------------------------\n");
//        eda.ShowSummeryDataset();
//        System.out.println("---------------------------\n---------------------------\n---------------------------\n");
//        eda.ShowDatasetDescribe();
//        eda.RemoveDuplicate();
//        eda.DropNullValue();
//        jobList = JobDAOImpl.ConvertRowDatasetToList(eda.getRowDataset());
//        System.out.println("---------------------------\n---------------------------\n---------------------------\n");
//
//        Map<String, Long> jobMap = eda.GetSortedCompanyJobsCount(jobList);
//        JobVisualization.PieChartCompanyJobsCount(jobMap);
//        System.out.println("---------------------------\n---------------------------\n---------------------------\n");
//
//        Map<String, Long> jobMap = eda.GetSortedJobsTitleCount(jobList);
//        JobVisualization.CategoryChartJobsCount(jobMap);
//        System.out.println("---------------------------\n---------------------------\n---------------------------\n");
//
//        Map<String, Long> jobMap = eda.GetSortedAreaCount(jobList);
//        JobVisualization.PieChartAreaJobsCount(jobMap);
//        System.out.println("---------------------------\n---------------------------\n---------------------------\n");
//
//        Map<String, Long> skillsMap = eda.GetSortedSkillsCount(jobList);
//        JobVisualization.PieChartSkillsCount(skillsMap);
//        System.out.println("---------------------------\n---------------------------\n---------------------------\n");
//        String[] ColumnsName = {"Title", "Company"}; //Skills
//        Dataset<Row> encoded = ai.java.hotencoder.OneHotEncoding.GetOneHotEncoding(sparkSession, eda.getRowDataset(ColumnsName),
//                ColumnsName);
//        String[] EncodedColumnsName = {"TitleIndex", "CompanyIndex"};
//        encoded = EDA.getRowDataset(encoded, EncodedColumnsName);
//        List<SampleData> samples = SampleData.GetSamplesListFromRowList(encoded);
//        Map<Centroid, List<SampleData>> clusters = KMean.fit(samples, 10, new EuclideanDistance(), 1000);
//        KMean.SparkKmeans(sparkSession, encoded, 10, 1000);
    }

    /**
     *
     * @param jobs
     */
    public static void chartCompany(List<Job> jobs) {
        Map<String, Long> valuesMap = eda.GetSortedCompanyJobsCount(jobs);
        JobVisualization.PieChartCompanyJobsCount(valuesMap);
    }

    /**
     *
     * @param jobs
     */
    public static void chartJob(List<Job> jobs) {
        Map<String, Long> valuesMap = eda.GetSortedJobsTitleCount(jobs);
        JobVisualization.CategoryChartJobsCount(valuesMap);
    }

    /**
     *
     * @param jobs
     */
    public static void chartArea(List<Job> jobs) {
        Map<String, Long> valuesMap = eda.GetSortedAreaCount(jobs);
        JobVisualization.PieChartAreaJobsCount(valuesMap);
    }

    /**
     *
     * @param jobs
     */
    public static void chartSkill(List<Job> jobs) {
        Map<String, Long> valuesMap = eda.GetSortedSkillsCount(jobs);
        JobVisualization.PieChartSkillsCount(valuesMap);
    }
}
