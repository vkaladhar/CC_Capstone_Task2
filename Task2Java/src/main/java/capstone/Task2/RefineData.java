package capstone.Task2;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
public class RefineData {
	
	private static String cvsSplitBy = ",";
	private static JavaSparkContext sc ;
	    
	
	
	private static final FlatMapFunction<String, String> DATA_EXTRACTOR =
      new FlatMapFunction<String, String>() {
		
		private static final long serialVersionUID = 1982716554529344518L;
		List<String> alist = null;
       
        public Iterable<String> call(String s) throws Exception {
	        	String[] values = s.split(cvsSplitBy);
	        	StringBuilder out = new StringBuilder();
	        	if(!values[0].contains("\"AirlineID\"")){
		        	if( values[10] != null && !values[10].equals("null")){
		        	out.append(values[0]).append(",");
		        	out.append(prepMsg(values[1])).append(",");
		        	out.append(prepMsg(values[2])).append(",");
		        	out.append(values[3]).append(",");
					out.append(prepMsg(values[4])).append(",");
					out.append(prepMsg(values[5])).append(",");
					out.append(prepMsg(values[6])).append(",");
					out.append(values[7]).append(",");
					out.append(prepMsg(values[8])).append(",");
					out.append(values[9]).append(",");
					out.append(values[10]);
					alist = Arrays.asList(out.toString());
		        	}
	        	}
	          return alist;
        }
        
        private String prepMsg(String val)
		{
			String retVal = "null";
			if(val !=null && val.trim().length() >0 ){
				if(val.startsWith("\"") || val.endsWith("\""))
					retVal = val.replaceAll("\"", "");
				else
					retVal = val;
			}
						
			return retVal;
		}
      };
	
	public static JavaRDD<String> run(JavaSparkContext sc, String t){
		
		JavaRDD<String> output = null;
		JavaRDD<String> inputFile = sc.textFile(t,1).repartition(1);
		output = inputFile.flatMap(DATA_EXTRACTOR);
			
        //JavaRDD<String> output_rdd = sc.parallelize(output);
	    return output;
	}
	
	
	public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Task2").setMaster("local"));
        
        //RefineData refJob = new RefineData(sc);
        JavaRDD<String> output_rdd = run(sc, "C:\\MyCourses\\CloudComputing\\capstone\\data\\CleanedData\\files");
        
        output_rdd.saveAsTextFile("C:\\MyCourses\\Output\\RefinedData");
        sc.close();
    }

}

