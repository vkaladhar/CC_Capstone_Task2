package capstone.Task2;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;


public class Group2_1 {
	private static final Log LOG = LogFactory.getLog(Group2_1.class);	
	static final String cvsSplitBy = ",";
	
	private static final PairFunction<String, String, Tuple2<Integer, Double>> AIRLINES_MAPPER =
      new PairFunction<String, String, Tuple2<Integer, Double>>() {
      	private static final long serialVersionUID = 1L;

		//@Override
		
        public Tuple2<String, Tuple2<Integer, Double>> call(String s) throws Exception {
        	//Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>("",0);
        	String airlineCode = "";
        	String airportCode = "";
        	Double depTimeDelay = 0.00;
        	String[] values = s.split(cvsSplitBy);
        	//Tuple2<Integer, Double> tempTuple2 = new Tuple2<Integer, Double>(0,0.00); 
        	
        	if(values[1] != null && values[1].trim().length() > 0){ //Dept
        		airlineCode = values[1];
        	}
        	if(values[4] != null && values[4].trim().length() > 0){ //Dept
        		airportCode = values[4];
        	}
        	String newKey = airportCode+"-"+airlineCode;
        	
        	if(values[7] != null && values[7].trim().length() > 0){ //Dept
        		depTimeDelay = new Double(values[7]);
        	}
        	
			
        	return new Tuple2<String, Tuple2<Integer, Double>>(newKey, new Tuple2<Integer, Double>(1,depTimeDelay));
        }
      };
		      
	  private static final Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> AIRLINES_REDUCER =
	  new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
	   
		private static final long serialVersionUID = -885163574906850829L;
	
		//@Override
	    public Tuple2<Integer, Double> call(Tuple2<Integer, Double> itemA, Tuple2<Integer, Double> itemB) throws Exception {
	    	//sum ItemA
	    	//sum ItemN
	    	Integer sumItemA = itemA._1+itemB._1;
	    	Double sumItemB = itemA._2+itemB._2;
	      return new Tuple2<Integer, Double>(sumItemA,sumItemB);
	    }
	  };
	
      
      private static final PairFunction<Tuple2<String,Tuple2<Integer,Double>>, String, Double> CALC_AVG_DEP_TIME =
	      new PairFunction<Tuple2<String,Tuple2<Integer,Double>>, String, Double>() {
	      	
			private static final long serialVersionUID = 1036534050635738112L;

			//@Override
	      	public Tuple2<String, Double> call(Tuple2<String,Tuple2<Integer,Double>> item) throws Exception {
	      			Double avgDepDelay = new BigDecimal(item._2._2/item._2._1).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
	      			String apCodeAndLine[] = item._1.split("-");
	      			return new Tuple2<String,Double>(apCodeAndLine[0]+"-"+apCodeAndLine[1],avgDepDelay);
	      		    //return tempTuple;
	           }

    	        
      };
      
     
      
      
      protected static class TupleComparator implements Comparator<Tuple2<Double, String>>, Serializable {
    	
		private static final long serialVersionUID = -6891927886649661199L;

			//@Override
    	    public int compare(Tuple2<Double, String> tuple1, Tuple2<Double, String> tuple2) {
    	        return tuple1._1 < tuple2._1 ? 0 : 1;
    	    }
    	}
      

      
      private static final FlatMapFunction<Tuple2<String, Double>, Tuple3<String,String,Double>> cassData =
    	    	new FlatMapFunction<Tuple2<String, Double>, Tuple3<String,String,Double>>() {
    	    	private static final long serialVersionUID = -5930457646921726966L;
    	    	
    	    	//int cnt =0;
    			public Iterable<Tuple3<String,String,Double>> call(Tuple2<String, Double> item) throws Exception {
    				//cnt++;
    				return Arrays.asList(new Tuple3<String,String,Double>(item._1.split("-")[0],item._1.split("-")[1],item._2));
    	    		
    	    	  }
    	    	  
    	      };
      
	   
            
      public static void main(String[] args) {
    	  
//        if (args.length <	 1) {
//          System.err.println("Please provide the input file full path as argument");
//          System.exit(0);
//        }

    	  
		Cluster cluster;
		Session session;
		cluster = Cluster.builder().addContactPoint("172.31.28.69").build();
		session = cluster.connect("capstone");
		
    	  
        SparkConf conf = new SparkConf().setAppName("capstone.Task2.AirlinesRanking")
        					.setMaster("local")
        					.set("spark.cassandra.connection.host", "172.31.28.69");;
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> file = RefineData.run(sc, "file:/usr/local/spark/data/Input-small");
        
        JavaPairRDD<String, Tuple2<Integer, Double>> airlinePairs = file.mapToPair(AIRLINES_MAPPER);
        JavaPairRDD<String, Tuple2<Integer, Double>> airlineReducer = airlinePairs.reduceByKey(AIRLINES_REDUCER);
                    
        
        
        JavaPairRDD<String, Double> airLineAvgDepTime = airlineReducer.mapToPair(CALC_AVG_DEP_TIME).sortByKey(true);
        
        JavaRDD<Tuple3<String,String,Double>> cassTuple = airLineAvgDepTime.flatMap(cassData);
               
                    
        CassandraJavaUtil.javaFunctions(cassTuple).writerBuilder("capstone", "group2_1", null).saveToCassandra();
        
        cassTuple.saveAsTextFile("/usr/local/spark/data/Output/Java/Group2_1");
        sc.close();
      }

}
