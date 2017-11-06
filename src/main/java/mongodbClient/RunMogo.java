package mongodbClient;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class RunMogo {
	private static MongoCollection<Document> collection;
	private static  MongoClient monClient;
	private static String format="yyyy-MM-dd HH:mm:ss";
	
	public static String test(){
		return format;
	}
	public static String  init(String hostList,String userName,String passwd,String database,String collectionName){
		String tranPasswd="";
		try {
			tranPasswd = java.net.URLEncoder.encode("jkfg#$%12", "utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		MongoClientURI uri = new MongoClientURI("mongodb://"+userName+":"+tranPasswd+"@"+hostList+"/"+database);
		MongoClientURI uri = new MongoClientURI("mongodb://rept_readonly:"+tranPasswd+"@172.16.30.41:27017,172.16.30.49:27016/bd_rept");
	    monClient = new MongoClient(uri);
	   
	   
		MongoDatabase db=monClient.getDatabase(database);	    
	    collection = db.getCollection(collectionName);
	    return tranPasswd;
	}
	
	public static String execSql(String filter_brandID,String filter_shopID,String filter_tradeStatus,String filter_businessType,
			String filter_tradePayStatus,String filter_calendarDateStart,String filter_calendarDateEnd){
				
				//解析数据
				ParaData pd=new ParaData();
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
				
//				//执行
//				Document doc=collection.find(
//			    		Filters.and(
////			    				Filters.eq("brand_identy", 7695)
//			    				Filters.eq("brand_identy", Integer.parseInt(filter_brandID)),
//			    				Filters.in("shop_identy", shopList),
//			    				Filters.in("trade_status", tradeStatusList),
//			    				Filters.in("business_type", businessTypeList),
//			    				Filters.in("trade_pay_status", tradePayStatusList),
//			    				Filters.gte("calendar_date",gteDate),
//			    				Filters.lte("calendar_date",lteDate)
//			    				)
//			    		).first();
//				String result=doc.toJson();
//				monClient.close();
//				return result;
				return filter_brandID;
	}
}
