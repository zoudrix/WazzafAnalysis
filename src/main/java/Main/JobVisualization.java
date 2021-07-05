package Main;


import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import java.util.ArrayList;
import java.util.Map;

public class JobVisualization {

    /// need to get from parent
    private static void PieChartAddSeries(PieChart chart, Map<String, Long> result) {
        for (Map.Entry<String, Long> entry : result.entrySet()) {
            chart.addSeries(entry.getKey(), entry.getValue());
        }
    }

    /// need to get from parent
    private static void CategoryChartAddSeries(CategoryChart chart, Map<String, Long> result) {
        chart.addSeries("Company Jobs Count", new ArrayList<>(result.keySet()),
                new ArrayList<>(result.values()));
    }

    public static void PieChartCompanyJobsCount(Map<String, Long> result) {
        PieChart chart = new PieChartBuilder().width(800).height(600).title("Pie Company Jobs Count").build();
        PieChartAddSeries(chart, result);
        new SwingWrapper(chart).displayChart();
    }

    public static void CategoryChartJobsCount(Map<String, Long> result) {
        CategoryChart chart = new CategoryChartBuilder().width(1024).height(768).title("Jobs Count")
                .xAxisTitle("Job").yAxisTitle("Count").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);
        CategoryChartAddSeries(chart, result);
        new SwingWrapper(chart).displayChart();
    }

    public static void PieChartAreaJobsCount(Map<String, Long> result) {
        PieChart chart = new PieChartBuilder().width(800).height(600).title("Pie Area Jobs Count").build();
        PieChartAddSeries(chart, result);
        new SwingWrapper(chart).displayChart();
    }

    public static void PieChartSkillsCount(Map<String, Long> result) {
        PieChart chart = new PieChartBuilder().width(800).height(600).title("Skills Count").build();
        PieChartAddSeries(chart, result);
        new SwingWrapper(chart).displayChart();
    }
}
