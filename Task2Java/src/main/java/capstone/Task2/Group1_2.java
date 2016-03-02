package capstone.Task2;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

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

public class Group1_2 {
	private static final Log LOG = LogFactory.getLog(Group1_2.class);	
	static final String cvsSplitBy = ",";
	
	private static final PairFunction<String, String, Integer> AIRLINES_MAPPER =
      new PairFunction<String, String, Integer>() {
      	private static final long serialVersionUID = 1L;

		//@Override
		
        public Tuple2<String, Integer> call(String s) throws Exception {
        	//Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>("",0);
        	String airlineCode = "";
        	String[] values = s.split(cvsSplitBy);
        	//System.out.println("Value4:"+values[4]);
        	//System.out.println("Value5:"+values[5]);
        	if(values[1] != null && values[1].trim().length() > 0){ //Dept
        		airlineCode = values[1];
        	}
			
        	return new Tuple2<String, Integer>(airlineCode, 1);
        }
      };
		      
	  private static final Function2<Integer, Integer, Integer> AIRLINES_REDUCER =
	  new Function2<Integer, Integer, Integer>() {
	   
		private static final long serialVersionUID = -885163574906850829L;
	
		//@Override
	    public Integer call(Integer a, Integer b) throws Exception {
	      return a + b;
	    }
	  };
	
	private static final PairFunction<String, String, Double> AIRLINES_MAPPER_ARRIVALTIME =
      new PairFunction<String, String, Double>() {
      	private static final long serialVersionUID = 1L;

		//@Override
		
        public Tuple2<String, Double> call(String s) throws Exception {
        	
        	String airlineCode = "";
        	Double ariivalDelay = 0.0;
        	String[] values = s.split(cvsSplitBy);
        	
        	if(values[1] != null && values[1].trim().length() > 0){ //Dept
        		airlineCode = values[1];
        		ariivalDelay = new Double(values[9]);
        	}
			return new Tuple2<String, Double>(airlineCode, ariivalDelay);
        }
      };
      
      private static final Function2<Double, Double, Double> AIRLINES_REDUCER_ARRIVALTIME =
      new Function2<Double, Double, Double>() {
       
		private static final long serialVersionUID = -885163574906850829L;

		//@Override
        public Double call(Double a, Double b) throws Exception {
          return a + b;
        }
      };
      
      private static final PairFunction<Tuple2<String,Tuple2<Integer,Double>>, String, Double> CALC_AVG_ARRVL_TIME =
	      new PairFunction<Tuple2<String,Tuple2<Integer,Double>>, String, Double>() {
	      	
			private static final long serialVersionUID = 1036534050635738112L;

			//@Override
	      	public Tuple2<String, Double> call(Tuple2<String,Tuple2<Integer,Double>> item) throws Exception {
	      			Double avgArrivalDelay = new BigDecimal(item._2._2/item._2._1).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
	      			Tuple2<String,Double> tempTuple = new Tuple2<String,Double>(item._1,avgArrivalDelay);
	      		    return tempTuple;
	           }

    	        
      };
      
      private static final PairFunction<Tuple2<String, Double>, Double, String> SWAP_MAPPER =
    	      new PairFunction<Tuple2<String, Double>, Double, String>() {
    	      	
    			private static final long serialVersionUID = 1036534050635738112L;

				//@Override
    	      	public Tuple2<Double, String> call(Tuple2<String, Double> item) throws Exception {
    	               return item.swap();
    	           }

    	        
      };
      
      private static final PairFunction<Tuple2<Double, String>, String, Double> SWAP_BACK =
    	      new PairFunction<Tuple2<Double, String>, String, Double>() {
    	      	
    			private static final long serialVersionUID = 1036534050635738112L;

				//@Override
    	      	public Tuple2<String, Double> call(Tuple2<Double, String> item) throws Exception {
    	               return item.swap();
    	           }

    	        
      };
      
      protected static class TupleComparator implements Comparator<Tuple2<Double, String>>, Serializable {
    	
		private static final long serialVersionUID = -6891927886649661199L;

			//@Override
    	    public int compare(Tuple2<Double, String> tuple1, Tuple2<Double, String> tuple2) {
    	        return tuple1._1 < tuple2._1 ? 0 : 1;
    	    }
    	}
      
      private static final FlatMapFunction<Tuple2<String, Double>, String> BracketRemover =
    	new FlatMapFunction<Tuple2<String, Double>, String>() {
    	private static final long serialVersionUID = -5930457646921726966L;
    	
    	int cnt =0;
		public Iterable<String> call(Tuple2<String, Double> item) throws Exception {
			cnt++;
			return Arrays.asList(item._1+","+item._2+","+cnt);
    		
    	  }
    	  
      };
      
            
      public static void main(String[] args) {
    	  
//        if (args.length <	 1) {
//          System.err.println("Please provide the input file full path as argument");
//          System.exit(0);
//        }

        SparkConf conf = new SparkConf().setAppName("capstone.Task2.Top10Airports").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file = RefineData.run(sc, "file:/usr/local/spark/data/Input-small");
        
        JavaPairRDD<String, Integer> airlinePairs = file.mapToPair(AIRLINES_MAPPER);
        JavaPairRDD<String, Integer> airlineReducer = airlinePairs.reduceByKey(AIRLINES_REDUCER);
        
        JavaPairRDD<String, Double> airlinePairsArrivalTime = file.mapToPair(AIRLINES_MAPPER_ARRIVALTIME);
        JavaPairRDD<String, Double> airlineReducerArrivalTime = airlinePairsArrivalTime.reduceByKey(AIRLINES_REDUCER_ARRIVALTIME);
        
        JavaPairRDD<String, Tuple2<Integer,Double>> airlineData = airlineReducer.join(airlineReducerArrivalTime);
        
        JavaPairRDD<String, Double> airLineAvgArrival = airlineData.mapToPair(CALC_AVG_ARRVL_TIME);
        
        JavaPairRDD<Double, String> swappedPair = airLineAvgArrival.mapToPair(SWAP_MAPPER).sortByKey(true);
        
        
        List<Tuple2<Double, String>> top10Tuple = swappedPair.top(10, new TupleComparator());
        
        JavaRDD<Tuple2<Double, String>> tempRdd = sc.parallelize(top10Tuple);
        
       JavaPairRDD<String, Double> tempRdd2 = tempRdd.mapToPair(SWAP_BACK);
        
       JavaRDD<String> finalRdd = tempRdd2.flatMap(BracketRemover);
        
       
       finalRdd.saveAsTextFile("/usr/local/spark/data/Output/Java/Group1_2");
        sc.close();
      }

}
