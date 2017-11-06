package mongodbClient;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.util.JSON;

public class MogoTest extends AbstractJavaSamplerClient  implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4224968300554123531L;
	private Logger log = Logger.getRootLogger();
	private SampleResult results;
	private long testCount=1;
	
	private String hostList;
	private String collectionName;
	private String passwd;
	private String userName;
	private String database;
	private MongoCollection<Document> collection;
	private MongoClient monClient=null;
	
	private String assertKeyWords;
	private String pageNum;
	private String queryType;//1,find��ѯ���������ܵ��ۺϲ�ѯ
	
	private int pageCount;
	private String filter_brandID;
	private String filter_shopID;
	private String filter_tradeStatus;
	private String filter_businessType;
	private String filter_tradePayStatus;
	private String filter_calendarDateStart;
	private String filter_calendarDateEnd;
	private String format="yyyy-MM-dd HH:mm:ss";
	
	private String aggregate_group;
	private long initTime=1;
	/**
	 * ���Գ�ʼ��
	 */
	public void setupTest(JavaSamplerContext context){
		listParameters(context);
		
	}
	
	/**
	 * ��ȡ����
	 */
	public Arguments getDefaultParameters() {
		Arguments params = new Arguments();
		params.addArgument( "hostList" , "" );
		params.addArgument( "collectionName" , "" );
		params.addArgument( "passwd" , "" );
		params.addArgument( "userName" , "" );
		params.addArgument( "database" , "" );
		params.addArgument( "queryType" , "" );
		
		
		params.addArgument( "filter_brandID" , "" );
		params.addArgument( "filter_shopID" , "" );
		params.addArgument( "filter_tradeStatus" , "" );
		params.addArgument( "filter_businessType" , "" );
		params.addArgument( "filter_tradePayStatus" , "" );
		params.addArgument( "filter_calendarDateStart" , "" );
		params.addArgument( "filter_calendarDateEnd" , "" );
		
		params.addArgument( "aggregate_group" , "" );
		params.addArgument( "assertKeyWords" , "" );
		params.addArgument( "pageNum" , "" );
		
		return params;
		
	}
	
	/**
     * �ú�����Ҫ������ѭ����ȡ�����б��е�����
     * û����������������޷�ʵ�ֶԲ�����ѭ������
     * @param context
     */
	private void listParameters(JavaSamplerContext context) {
	
	      String name;
	      for ( Iterator argsIt= context.getParameterNamesIterator(); argsIt.hasNext(); log.info(( new StringBuilder()).append(name)
	   .append( "=" ).append(context.getParameter(name)).toString()))
	    	  name= (String) argsIt.next();
	         
	   }

	/**
	 * �� jmeter �������洫��Ĳ�����ֵ������
	 * @param context
	 */
	private void setupValues(JavaSamplerContext context) {
	
		hostList=context.getParameter( "hostList" , "" );
		collectionName=context.getParameter( "collectionName" , "" );
		passwd=context.getParameter( "passwd" , "" );
		userName=context.getParameter( "userName" , "" );
		database=context.getParameter( "database" , "" );
		queryType=context.getParameter( "queryType" , "1" );
		aggregate_group=context.getParameter( "aggregate_group", "1" );
		
		filter_brandID=context.getParameter( "filter_brandID" , "" );
		filter_shopID=context.getParameter( "filter_shopID" , "" );
		filter_tradeStatus=context.getParameter( "filter_tradeStatus" , "" );
		filter_businessType=context.getParameter( "filter_businessType" , "" );
		filter_tradePayStatus=context.getParameter( "filter_tradePayStatus" , "" );
		filter_calendarDateStart=context.getParameter( "filter_calendarDateStart" , "" );
		filter_calendarDateEnd=context.getParameter( "filter_calendarDateEnd" , "" );
		
		assertKeyWords=context.getParameter( "assertKeyWords" , "_id" );
		pageNum=context.getParameter( "pageNum" , "20" );
		

	}
	
	/**
	 * ����ִ��
	 */
	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		// TODO Auto-generated method stub
		results = new SampleResult();
		listParameters(context);
		setupValues(context);
		//��ʼ������
		if(initTime==1){
			pageCount=Integer.parseInt(pageNum);
//			pageCount=20;
			String tranPasswd="";
			try {
				tranPasswd = java.net.URLEncoder.encode(passwd, "utf-8");
				log.info("passwdUri="+tranPasswd);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			log.info("start init mongodb connection����");
			MongoClientURI uri = new MongoClientURI("mongodb://"+userName+":"+tranPasswd+"@"+hostList+"/"+database);
			log.info("mogoUri:"+uri);
		    monClient = new MongoClient(uri);
		    //设置集群读取策略：优先读取从节点
		    monClient.setReadPreference(ReadPreference.secondaryPreferred());
		    MongoDatabase db=monClient.getDatabase(database);
		    
		    collection = db.getCollection(collectionName);
		    initTime++;
		}
		
		//�жϲ�ѯ���ͣ����в�ѯ
		ParaData pd=new ParaData();
		Iterable<Document> resDoc;
		Document firstDoc=null;
		long start;
		String separator=System.getProperty("line.separator");
		if(queryType.equals("1")){
			//������ѯ����
			ArrayList<Integer> shopList=pd.getList(filter_shopID);
			ArrayList<Integer> tradeStatusList=pd.getList(filter_tradeStatus);
			ArrayList<Integer> businessTypeList=pd.getList(filter_businessType);
			ArrayList<Integer> tradePayStatusList=pd.getList(filter_tradePayStatus);
			
		    SimpleDateFormat sdf = new SimpleDateFormat(format);
		    Date gteDate=null;
		    Date lteDate=null;
			try {
				gteDate=sdf.parse(filter_calendarDateStart);
				lteDate=sdf.parse(filter_calendarDateEnd);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			log.info("gteDate="+gteDate.toGMTString()+",lteDate="+lteDate.toGMTString());
			start=System.currentTimeMillis();
			//ִ��
			results.sampleStart();
			results.setSamplerData(
					new StringBuffer("brand_identy:").append(filter_brandID).append(separator).
					append("shop_identy:").append(filter_shopID).append(separator).
					append("trade_status:").append(filter_tradeStatus).append(separator).
					append("business_type:").append(filter_businessType).append(separator).
					append("trade_pay_status:").append(filter_tradePayStatus).append(separator).
					append("calendar_date:").append(filter_calendarDateStart).append(",").append(filter_calendarDateEnd).
					toString()
					);
			
			resDoc=collection.find(
		    		Filters.and(
		    				Filters.eq("brand_identy", Integer.parseInt(filter_brandID)),
		    				Filters.in("shop_identy", shopList),
		    				Filters.in("trade_status", tradeStatusList),
		    				Filters.in("business_type", businessTypeList),
		    				Filters.in("trade_pay_status", tradePayStatusList),
		    				Filters.gte("calendar_date",gteDate),
		    				Filters.lte("calendar_date",lteDate)
		    				)
		    		);
		}else{
			start=System.currentTimeMillis();
//			String aggregate_match=pd.getMatchStr(filter_brandID, filter_shopID, filter_tradeStatus, filter_businessType, filter_tradePayStatus, filter_calendarDateStart,"");
			//ִ��
			results.sampleStart();
			BasicDBObject match = pd.getMatchBson(filter_brandID, filter_shopID, filter_tradeStatus, filter_businessType, filter_tradePayStatus, filter_calendarDateStart, filter_calendarDateEnd, format);
			BasicDBObject group =pd.getGroupBson();
			results.setSamplerData(new StringBuffer("match:").append(match).append(separator).append("group:").append(group).toString());
		    List<BasicDBObject> aggr=new ArrayList<BasicDBObject>();
		    aggr.add(match);
		    aggr.add(group);
		    resDoc=collection.aggregate(aggr, Document.class);
		}
//		//������ѯ����
//		ArrayList<Integer> shopList=ParaData.getList(filter_shopID);
//		ArrayList<Integer> tradeStatusList=ParaData.getList(filter_tradeStatus);
//		ArrayList<Integer> businessTypeList=ParaData.getList(filter_businessType);
//		ArrayList<Integer> tradePayStatusList=ParaData.getList(filter_tradePayStatus);
//		
//	    SimpleDateFormat sdf = new SimpleDateFormat(format);
//	    Date gteDate=null;
//	    Date lteDate=null;
//		try {
//			gteDate=sdf.parse(filter_calendarDateStart);
//			lteDate=sdf.parse(filter_calendarDateEnd);
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		log.info("gteDate="+gteDate.toGMTString()+",lteDate="+lteDate.toGMTString());
//		long start=System.currentTimeMillis();
//		//ִ��
//		results.sampleStart();
//		results.setSamplerData(filter_shopID);
//		MongoIterable<Document> mi=collection.find(
//	    		Filters.and(
//	    				Filters.eq("brand_identy", Integer.parseInt(filter_brandID)),
//	    				Filters.in("shop_identy", shopList),
//	    				Filters.in("trade_status", tradeStatusList),
//	    				Filters.in("business_type", businessTypeList),
//	    				Filters.in("trade_pay_status", tradePayStatusList),
//	    				Filters.gte("calendar_date",gteDate),
//	    				Filters.lte("calendar_date",lteDate)
//	    				)
//	    		);
		
		//�������
		int first=0;
	    for(Document doc:resDoc){
	    	if(first>0)break;
	    	firstDoc=doc;
	    	first++;
	    }
	    long firstDocTime=System.currentTimeMillis();;
	    
//		StringBuffer res=new StringBuffer();
		StringBuffer docNum=new StringBuffer("The document's count is:");
	    int num=0;
	    long size=0;
	    for(Document doc:resDoc){
	    	size+=doc.toJson().getBytes().length;
//	    	if(++num%pageCount==0){
//	    		res.delete(0, res.length());
////	    		res=new StringBuffer();
//	    	}
	    }
	    long end=System.currentTimeMillis();
//	    StringBuffer duration=new StringBuffer("The query duration time:").append(end-start).append("ms");
//	    log.info(duration.toString());
	    String result="";
	    boolean resIsNull=false;
	    try{
	    	result=docNum.append(num).append(separator)
					.append("The responseData size is:").append(((float)size)/1024/1024).append("MB").append(separator)
					.append("The query duration time:").append(firstDocTime-start).append("ms").append(separator)
					.append("The transaction duration time:").append(end-start).append("ms").append(separator)
					.append("The first Document is:").append(firstDoc.toJson()).toString();
	    }catch(NullPointerException e){
	    	resIsNull=true;
	    	log.info("��ѯ���Ϊ��");
	    	StringBuffer errInfo=new StringBuffer();
	    	for(StackTraceElement ele:e.getStackTrace()){
	    		errInfo.append(ele.toString());
	    	}
	    	log.info(errInfo.toString());
	    }
		
		
		results.setResponseData(result, null );
		log.info("assertKeyWords:"+assertKeyWords+"query time:"+(firstDocTime-start));
		if(result.contains(assertKeyWords)){
			log.info(testCount+"-Query successfully!");
			results.setSuccessful( true );
	        results.setResponseCode( "1000" );
	        results.setResponseMessage( "��ѯ�ɹ�" );
		}else if(resIsNull){
			log.info(testCount+"-Query result is Null!");
			results.setSuccessful( false );
	        results.setResponseCode( "3000" );
	        results.setResponseMessage( "��ѯ�ɹ��������Ϊ��" );
		}else{
			log.info(testCount+"-Query failed!"+(firstDocTime-start));
			results.setSuccessful(false);
	        results.setResponseCode("2000");
	        results.setResponseMessage( "��ѯʧ��" );
		}
	    results.setDataEncoding( "utf-8" );
	    results.setDataType( "text" );
	    results.sampleEnd();
	    testCount++;
		return results;
	}
	
	/**
	 * ������ɺ󣬻����ָ�
	 */
	public void teardownTest(JavaSamplerContext arg0 ){
		monClient.close();
	}

}
