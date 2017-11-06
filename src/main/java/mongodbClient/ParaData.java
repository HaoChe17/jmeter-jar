package mongodbClient;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoNamespace;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.util.JSON;

public class ParaData {
	
	public static void main(String[] args) {
//		RunMogo.init("172.16.30.49:27016,172.16.30.41:27017","rept_readonly","jkfg#$%12","bd_rept","mongo_mind_goods_sale_count_hour");
//		String res=RunMogo.execSql("7695","810006582,810006583","4,5,11,12,10","1,2,3,6,8,15,16","1,2,3,4,5,6,8","2011-7-25 8:0:0","2017-8-25 8:0:0");
//		try {
//			System.out.println(java.net.URLEncoder.encode("jkfg#$%12", "utf-8"));
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println(new ParaData().getMatchStr("7695","810006582","4","2","5","2017,7,25,8,0,0","2017,8,30,8,0,0"));
//		new ParaData().mongodb();
		long a=123444585;
		System.out.println("a="+((float)a)/1024/1024);
//		new ParaData().mogoAggregate();
	}
	
	/**
	 * mongodb的find查询
	 */
	public void mongodb(){
		//设置连接池
//		MongoClientOptions.Builder build = new MongoClientOptions.Builder().
//				connectionsPerHost(50).//与数据最大连接数50
//				threadsAllowedToBlockForConnectionMultiplier(50).//如果当前所有的connection都在使用中，则每个connection上可以有50个线程排队等待
//				threadsAllowedToBlockForConnectionMultiplier(50).
//				connectTimeout(1*60*1000).
//				maxWaitTime(2*60*1000);
//	    
//	    //MongoClientOptions options = build.build();
		String passwd="";
		try {
			passwd=java.net.URLEncoder.encode("jkfg#$%12", "utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//	    MongoClientURI uri = new MongoClientURI("mongodb://rept_readonly:"+passwd+"@172.16.30.41:27017,172.16.30.49:27016/bd_rept",build);
	    MongoClientURI uri = new MongoClientURI("mongodb://rept_readonly:"+passwd+"@172.16.30.41:27017,172.16.30.49:27016/bd_rept");
	    MongoClient monClient = new MongoClient(uri);
	    MongoDatabase database=monClient.getDatabase("bd_rept");
	    
	    MongoCollection<Document> collection = database.getCollection("mongo_mind_goods_sale_count_hour");
	    
	    //Filters.all("shop_identy", "810006582","810006583");
	    String format= "yyyy-MM-dd HH:mm:ss";
	    String filter_brandID="7695";
	    String filter_shopID="810006582";
	    String filter_tradeStatus="4";;
	    String filter_businessType="2";
	    String filter_tradePayStatus="5";
	    String filter_calendarDateStart="2017-7-25 8:0:0";
	    String filter_calendarDateEnd="2019-8-30 8:0:0";
	    //解析查询数据
		ArrayList<Integer> shopList=getList(filter_shopID);
		ArrayList<Integer> tradeStatusList=getList(filter_tradeStatus);
		ArrayList<Integer> businessTypeList=getList(filter_businessType);
		ArrayList<Integer> tradePayStatusList=getList(filter_tradePayStatus);
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
		
		//执行
		Bson fileterBson=Filters.and(
				Filters.eq("_id", new ObjectId("59db0d0ee925d00dce8394fc")),
				Filters.eq("brand_identy", Integer.parseInt(filter_brandID)),
				Filters.in("shop_identy", shopList),
				Filters.in("trade_status", tradeStatusList),
				Filters.in("business_type", businessTypeList),
				Filters.in("trade_pay_status", tradePayStatusList),
				Filters.gte("calendar_date",gteDate),
				Filters.lte("calendar_date",lteDate)
				);
//		String sql=fileterBson.toBsonDocument(BsonDocument.class,(CodecRegistry)new DocumentCodecProvider()).toJson();
//		System.out.println("sql:"+sql);
		MongoIterable<Document> resDoc=collection.find(fileterBson);
	    StringBuffer res=new StringBuffer();
	    int num=0;
	    for(Document doc:resDoc){
	    	res.append(doc.toJson()).append(",");
	    	num++;
	    }
	    
	    System.out.println("document's number is: "+num);
	    System.out.println(res);
	    
	    monClient.close();
	    
	}
	
	/**
	 * mongodb的聚合运算
	 */
	public void mogoAggregate(){
		String passwd="";
		try {
			passwd=java.net.URLEncoder.encode("jkfg#$%12", "utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    MongoClientURI uri = new MongoClientURI("mongodb://rept_readonly:"+passwd+"@172.16.30.41:27017,172.16.30.49:27016/bd_rept");
	    MongoClient monClient = new MongoClient(uri);
	    MongoDatabase database=monClient.getDatabase("bd_rept");
	    
	    
	    MongoCollection<Document> collection = database.getCollection("mongo_mind_goods_sale_count_hour");
	    
//	    String matchValue="{$match:{\"brand_identy\":7695,\"shop_identy\":{$in:[810006582]},\"trade_status\":{$in:[4]},\"business_type\":{$in:[2]},\"trade_pay_status\":{$in:[5]}}}";
//	    String matchValue=getMatchStr("7695","810006582","4","2","5","2017-7-25 8:0:0","2017-12-30 8:0:0");
//	    String groupValue="{$group:{\"_id\":{calendar_date:\"$calendar_date\",sku_uuid:\"$sku_uuid\",sku_name:\"$sku_name\",sku_sale_times:\"$sku_sale_times\"},sku_sale_times:{$sum:\"$sku_sale_times\"}}}";
	    BasicDBObject group=getGroupBson();
	    BasicDBObject match = getMatchBson("7695","810006582","4","2","5","2017-7-25 8:0:0","2017-9-30 16:0:0","yyyy-MM-dd HH:mm:ss");
	    System.out.println(group);
	    System.out.println(match);
//	    BasicDBObject group = (BasicDBObject)JSON.parse(groupValue);
	    List<BasicDBObject> aggr=new ArrayList<BasicDBObject>();
	    aggr.add(match);
	    aggr.add(group);
	    
	    AggregateIterable<Document> aggResIte=collection.aggregate(aggr, Document.class);
	    StringBuffer res=new StringBuffer();
	    for(Document doc:aggResIte){
	    	res.append(doc.toJson());
	    }
	    System.out.println("result:"+res.toString());
	    monClient.close();
//	    MongoNamespace mogoName=new MongoNamespace("bd_rept","mongo_mind_goods_sale_count_hour");
//	    AggregateOperation aggrOper=new AggregateOperation(mogoName,aggr,resultDoc);
	}
	
	/**
	 * 把含有逗号的纯数字字符串转换为list数据
	 * @param para
	 * @return
	 */
	public ArrayList<Integer> getList(String para){
		ArrayList<Integer> list=new ArrayList<Integer>();
		String[] paraAraay=para.split(",");
		for(String str:paraAraay){
			list.add(Integer.valueOf(str));
		}
		return list;
	} 
	
	/**
	 * 生成match的bson格式数据
	 * @param brandID
	 * @param shopID
	 * @param tradeStatus
	 * @param businessType
	 * @param tradePayStatus
	 * @param calendarDateStart
	 * @param calendarDateEnd
	 * @param format
	 * @return
	 */
	public BasicDBObject getMatchBson(String brandID,String shopID,String tradeStatus,String businessType,String tradePayStatus,String calendarDateStart,String calendarDateEnd,String format){
		
		StringBuffer matchBuf=new StringBuffer();
		
		SimpleDateFormat sdf = new SimpleDateFormat(format);
	    Date gteDate=null;
	    Date lteDate=null;
		try {
			gteDate=sdf.parse(calendarDateStart);
			lteDate=sdf.parse(calendarDateEnd);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		matchBuf.append("{$match:{").
//		append("\"brand_identy\":").append(brandID).append(",").
//		append("\"shop_identy\":{$in:[").append(shopID).append("]},").
//		append("\"trade_status\":{$in:[").append(tradeStatus).append("]},").
//		append("\"business_type\":{$in:[").append(businessType).append("]},").
//		append("\"trade_pay_status\":{$in:[").append(tradePayStatus).append("]},").
//		append("\"calendar_date\":{$gte:\"").append(gteDate).append("\",").append("$lte:\"").append(lteDate).append("\"}").
//		append("}}");
		
		DBObject dateFields = new BasicDBObject();
		dateFields.put("$gte", gteDate);
		dateFields.put("$lte", lteDate);
		DBObject matchValue = new BasicDBObject();
		matchValue.put("brand_identy", Integer.parseInt(brandID));
		matchValue.put("shop_identy", new BasicDBObject("$in",getList(shopID)));
		matchValue.put("calendar_date", dateFields);
		matchValue.put("trade_status", new BasicDBObject("$in",getList(tradeStatus)));
		matchValue.put("business_type", new BasicDBObject("$in",getList(businessType)));
		matchValue.put("trade_pay_status", new BasicDBObject("$in",getList(tradePayStatus)));
//		matchValue.put("calendar_date", dateFields);
        
        
		return new BasicDBObject("$match",matchValue);
	}
	
	/**
	 * 生成group的bson格式数据
	 * @return
	 */
	public BasicDBObject getGroupBson(){
		BasicDBObject groupValue=new BasicDBObject();
		
		BasicDBObject idBson=new BasicDBObject();
		idBson.put("calendar_date", "$calendar_date");
		idBson.put("sku_uuid", "$sku_uuid");
		idBson.put("sku_name", "$sku_name");
		
		groupValue.put("_id", idBson);
		groupValue.put("sku_sale_times", new BasicDBObject("$sum","$sku_sale_times"));
		groupValue.put("sku_sale_count", new BasicDBObject("$sum","$sku_sale_count"));
		groupValue.put("property_amount", new BasicDBObject("$sum","$property_amount"));
		groupValue.put("sku_sale_amount", new BasicDBObject("$sum","$sku_sale_amount"));
		groupValue.put("sku_count_in_single", new BasicDBObject("$sum","$sku_count_in_single"));
		groupValue.put("sku_count_in_setmeal", new BasicDBObject("$sum","$sku_count_in_setmeal"));
		groupValue.put("single_discount_amount", new BasicDBObject("$sum","$single_discount_amount"));
		groupValue.put("gift_discount_amount", new BasicDBObject("$sum","$gift_discount_amount"));
		groupValue.put("member_discount_amount", new BasicDBObject("$sum","$member_discount_amount"));
		groupValue.put("activities_discount_amount", new BasicDBObject("$sum","$activities_discount_amount"));
		groupValue.put("apportion_single_discount_amount", new BasicDBObject("$sum","$apportion_single_discount_amount"));
		groupValue.put("apportion_gift_discount_amount", new BasicDBObject("$sum","$apportion_gift_discount_amount"));
		groupValue.put("apportion_member_discount_amount", new BasicDBObject("$sum","$apportion_member_discount_amount"));
		groupValue.put("apportion_activities_discount_amount", new BasicDBObject("$sum","$apportion_activities_discount_amount"));
		groupValue.put("sku_wholebill_privilege_amount", new BasicDBObject("$sum","$sku_wholebill_privilege_amount"));
		groupValue.put("sku_apportion_sale_amount", new BasicDBObject("$sum","$sku_apportion_sale_amount"));
		
		return new BasicDBObject("$group",groupValue);
	}
}
