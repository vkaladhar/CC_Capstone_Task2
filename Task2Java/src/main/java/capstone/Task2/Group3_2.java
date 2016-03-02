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

public class Group3_2 {
	private static final Log LOG = LogFactory.getLog(Group3_2.class);	
	static final String cvsSplitBy = ",";
	
	private static final FlatMapFunction<String, String> DATA_EXTRACTOR =
      new FlatMapFunction<String, String>() {
		List<String> alist = null;
		private static final long serialVersionUID = 1982716554529344518L;
		
		 public Iterable<String> call(String s) throws Exception {
        	String[] str = s.split(cvsSplitBy);
        	StringBuilder out = new StringBuilder();
        	
        	if ( Double.parseDouble(str[7]) >= -30  && Double.parseDouble(str[9]) >= -30)
        		if ( Double.parseDouble(str[7]) <= 30  && Double.parseDouble(str[9]) <= 30){
        			out.append(str[3]).append(",");
        			out.append(str[4]).append("-").append(str[5]).append(",");
        			out.append(str[1]).append("-").append(str[2]).append(",");
        			out.append(str[6]).append(",");
        			out.append(str[8]).append(",");
        			alist = Arrays.asList(out.toString());
        	}
        	return alist;

        }
	};
	
	private static final PairFunction<String, String, Tuple2<Integer, Double>> AIRLINES_MAPPER =
      new PairFunction<String, String, Tuple2<Integer, Double>>() {
      	private static final long serialVersionUID = 1L;

		//@Override
		
        public Tuple2<String, Tuple2<Integer, Double>> call(String s) throws Exception {
        	//Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>("",0);
        	String airlineCode = "";
        	String airportOrigCode = "";
        	String airportDestCode = "";
        	Double depTimeDelay = 0.00;
        	String[] values = s.split(cvsSplitBy);
        	//Tuple2<Integer, Double> tempTuple2 = new Tuple2<Integer, Double>(0,0.00); 
        	
        	if(values[1] != null && values[1].trim().length() > 0){ //Dept
        		airlineCode = values[1];
        	}
        	if(values[4] != null && values[4].trim().length() > 0){ //Dept
        		airportOrigCode = values[4];
        	}
        	if(values[5] != null && values[5].trim().length() > 0){ //Dept
        		airportDestCode = values[5];
        	}
        	String newKey = airportOrigCode+"-"+airportDestCode+"-"+airlineCode;
        	
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
	
	private static final PairFunction<String, String, Double> AIRLINES_MAPPER_DEPTIME =
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
      
      private static final Function2<Double, Double, Double> AIRLINES_REDUCER_DEPTIME =
      new Function2<Double, Double, Double>() {
       
		private static final long serialVersionUID = -885163574906850829L;

		//@Override
        public Double call(Double a, Double b) throws Exception {
          return a + b;
        }
      };
      
      private static final PairFunction<Tuple2<String,Tuple2<Integer,Double>>, String, Double> CALC_AVG_DEP_TIME =
	      new PairFunction<Tuple2<String,Tuple2<Integer,Double>>, String, Double>() {
	      	
			private static final long serialVersionUID = 1036534050635738112L;

			//@Override
	      	public Tuple2<String, Double> call(Tuple2<String,Tuple2<Integer,Double>> item) throws Exception {
	      			Double avgDepDelay = new BigDecimal(item._2._2/item._2._1).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
	      			String apCodeAndLine[] = item._1.split("-");
	      			return new Tuple2<String,Double>(apCodeAndLine[0]+"-"+apCodeAndLine[1]+","+apCodeAndLine[2],avgDepDelay);
	      		    //return tempTuple;
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
    	
    	//int cnt =0;
		public Iterable<String> call(Tuple2<String, Double> item) throws Exception {
			//cnt++;
			return Arrays.asList(item._1+","+item._2);
    		
    	  }
    	  
      };
      
            
      public static void main(String[] args) {
    	  
//        if (args.length <	 1) {
//          System.err.println("Please provide the input file full path as argument");
//          System.exit(0);
//        }

        SparkConf conf = new SparkConf().setAppName("capstone.Task2.AirlinesXYRanking").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> file = context.textFile("C:\\MyCourses\\CloudComputing\\capstone\\data\\CleanedData\\part-00000");
        
        JavaRDD<String> airlineData = file.flatMap(DATA_EXTRACTOR);
        
        //JavaPairRDD<String, Tuple2<Integer, Double>> airlineReducer = airlinePairs.reduceByKey(AIRLINES_REDUCER);
                    
        //JavaPairRDD<String, Double> airlinePairsArrivalTime = file.mapToPair(AIRLINES_MAPPER_DEPTTIME);
        //JavaPairRDD<String, Double> airlineReducerArrivalTime = airlineReducer.reduceByKey(AIRLINES_REDUCER_DEPTIME);
        
        //JavaPairRDD<String, Tuple2<Integer,Double>> airlineData = airlineReducer.join(airlineReducerArrivalTime);
        
        //JavaPairRDD<String, Double> airLineAvgDepTime = airlineReducer.mapToPair(CALC_AVG_DEP_TIME).sortByKey(true);
        
        //JavaPairRDD<Double, String> swappedPair = airLineAvgArrival.mapToPair(SWAP_MAPPER).sortByKey(true);
        
        
        //List<Tuple2<Double, String>> top10Tuple = swappedPair.top(10, new TupleComparator());
        
        //JavaRDD<Tuple2<Double, String>> tempRdd = context.parallelize(top10Tuple);
        
       //JavaPairRDD<String, Double> tempRdd2 = tempRdd.mapToPair(SWAP_BACK);
        
       //JavaRDD<String> finalRdd = airLineAvgDepTime.flatMap(BracketRemover);
       
        
        airlineData.saveAsTextFile("C:\\MyCourses\\Output\\Group3_2");
        context.close();
      }

}
