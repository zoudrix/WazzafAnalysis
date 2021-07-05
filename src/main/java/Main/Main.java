package Main;

import org.apache.spark.sql.SparkSession;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main
{
    public static void main(String[] args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder().appName("jobs").master("local[*]").getOrCreate();
//        String jobDatasetPath = "/media/mersahl/hdd/home/mersahl/workspace/iti-ai-pro/java-uml/JavaFinalProject/src/main/resources/Dataset/Wuzzuf_Jobs.csv";
        String jobDatasetPath = "./src/main/resources/Dataset/Wuzzuf_Jobs.csv";
        List<Job> jobList = new JobDAOImpl().ReadCSVFile(jobDatasetPath);
        //Dataset<Row> jobRow = new JobDAOImpl().SparkReadCSVFile(sparkSession, jobDatasetPath);

        EDA eda = new EDA(sparkSession);
        Dataset<Row> jobDataset = eda.ConvertJobsToDatasetRow(jobList);
        //System.out.println("---------------------------\n---------------------------\n---------------------------\n");
        //eda.ShowDataset();
        //System.out.println("---------------------------\n---------------------------\n---------------------------\n");
        //eda.ShowSummeryDataset();
        //System.out.println("---------------------------\n---------------------------\n---------------------------\n");
        //eda.ShowDatasetDescribe();
        //eda.RemoveDuplicate();
        //eda.DropNullValue();
        //jobList = JobDAOImpl.ConvertRowDatasetToList(eda.getRowDataset());
        //System.out.println("---------------------------\n---------------------------\n---------------------------\n");
        //Map<String, Long>  jobMap = eda.GetSortedCompanyJobsCount(jobList);
        //JobVisualization.PieChartCompanyJobsCount(jobMap);
        //System.out.println("---------------------------\n---------------------------\n---------------------------\n");
        //Map<String, Long> jobMap = eda.GetSortedJobsTitleCount(jobList);
        //JobVisualization.CategoryChartJobsCount(jobMap);
        //System.out.println("---------------------------\n---------------------------\n---------------------------\n");
        //Map<String, Long> jobMap = eda.GetSortedAreaCount(jobList);
        //JobVisualization.PieChartAreaJobsCount(jobMap);
        //System.out.println("---------------------------\n---------------------------\n---------------------------\n");
        //Map<String, Long> skillsMap = eda.GetSortedSkillsCount(jobList);
        //JobVisualization.PieChartSkillsCount(skillsMap);
        //String[] ColumnsName = {"Title", "Company"}; //Skills
        //Dataset<Row> encoded= HotEncoder.OneHotEncoding.GetOneHotEncoding(sparkSession, eda.getRowDataset(ColumnsName),
        //        ColumnsName);
        //String[] EncodedColumnsName = {"TitleIndex", "CompanyIndex"};
        //encoded = EDA.getRowDataset(encoded, EncodedColumnsName);
        //List<SampleData> samples = SampleData.GetSamplesListFromRowList(encoded);
        //Map<Centroid, List<SampleData>> clusters = KMean.fit(samples, 10, new EuclideanDistance(), 1000);
        //KMean.SparkKmeans(sparkSession, encoded, 10, 1000);
    }
}