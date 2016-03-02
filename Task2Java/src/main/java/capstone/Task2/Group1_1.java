package capstone.Task2;
import java.io.Serializable;
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

public class Group1_1 {
	private static final Log LOG = LogFactory.getLog(Group1_1.class);	
	static final String cvsSplitBy = ",";
	
	private static final PairFunction<String, String, Integer> AIRPORTS_MAPPER =
      new PairFunction<String, String, Integer>() {
      	private static final long serialVersionUID = 1L;

		//@Override
		
        public Tuple2<String, Integer> call(String s) throws Exception {
        	//Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>("",0);
        	String airportCode = "";
        	String[] values = s.split(cvsSplitBy);
        	//System.out.println("Value4:"+values[4]);
        	//System.out.println("Value5:"+values[5]);
        	if(values[4] != null && values[4].trim().length() > 0){ //Dept
				airportCode = values[4];
        	}
			if(values[5] != null && values[5].trim().length() > 0){ //Arrival
				airportCode = values[5];
			}
        	return new Tuple2<String, Integer>(airportCode, 1);
        }
      };
      
      private static final Function2<Integer, Integer, Integer> AIRPORT_REDUCER =
      new Function2<Integer, Integer, Integer>() {
       
		private static final long serialVersionUID = -885163574906850829L;

		//@Override
        public Integer call(Integer a, Integer b) throws Exception {
          return a + b;
        }
      };
      
      private static final PairFunction<Tuple2<String, Integer>, Integer, String> SWAP_MAPPER =
    	      new PairFunction<Tuple2<String, Integer>, Integer, String>() {
    	      	
    			private static final long serialVersionUID = 1036534050635738112L;

				//@Override
    	      	public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
    	               return item.swap();
    	           }

    	        
      };
      
      private static final PairFunction<Tuple2<Integer, String>, String, Integer> SWAP_BACK =
    	      new PairFunction<Tuple2<Integer, String>, String, Integer>() {
    	      	
    			private static final long serialVersionUID = 1036534050635738112L;

				//@Override
    	      	public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
    	               return item.swap();
    	           }

    	        
      };
      
      protected static class TupleComparator implements Comparator<Tuple2<Integer, String>>, Serializable {
    	
		private static final long serialVersionUID = -6891927886649661199L;

			//@Override
    	    public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
    	        return tuple1._1 > tuple2._1 ? 0 : 1;
    	    }
    	}
      
      private static final FlatMapFunction<Tuple2<String, Integer>, String> BracketRemover =
    	new FlatMapFunction<Tuple2<String, Integer>, String>() {
    	private static final long serialVersionUID = -5930457646921726966L;
    	List<String> alist = null;
    	int cnt =0;
		public Iterable<String> call(Tuple2<String, Integer> item) throws Exception {
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

        //JavaRDD<String> file = sc.textFile("file:/usr/local/spark/data/Input-small");
        
        JavaPairRDD<String, Integer> pairs = file.mapToPair(AIRPORTS_MAPPER);
        JavaPairRDD<String, Integer> counter = pairs.reduceByKey(AIRPORT_REDUCER);
        
        JavaPairRDD<Integer, String> swappedPair = counter.mapToPair(SWAP_MAPPER).sortByKey(false);
        List<Tuple2<Integer, String>> top10Tuple = swappedPair.top(10, new TupleComparator());
        
        JavaRDD<Tuple2<Integer, String>> tempRdd = sc.parallelize(top10Tuple);
        
        JavaPairRDD<String, Integer> tempRdd2 = tempRdd.mapToPair(SWAP_BACK);
        
        JavaRDD<String> finalRdd = tempRdd2.flatMap(BracketRemover);
        
        
        finalRdd.saveAsTextFile("file:/usr/local/spark/data/Output/Java/Group1_1");
        sc.close();
      }

}
