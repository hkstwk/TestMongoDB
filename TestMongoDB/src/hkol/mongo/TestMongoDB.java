package hkol.mongo;

import static java.util.Arrays.asList;

import java.text.ParseException;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import static com.mongodb.client.model.Filters.*;

import hkol.mongo.database.DBConnector;
import hkol.mongo.helper.PrintHelper;

public class TestMongoDB {
	
	public static void main(String[] args) throws ParseException {
		MongoClient mongoClient = DBConnector.getMongoClient("localhost", 27017);
		MongoDatabase db = DBConnector.getMongoDatabase(mongoClient, "test");
		MongoCollection<Document> coll = DBConnector.getCollection(mongoClient,db,"restaurants");
	
//		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
//		db.getCollection("restaurants").insertOne(
//		        new Document("address",
//		                new Document()
//		                        .append("street", "2 Avenue")
//		                        .append("zipcode", "10075")
//		                        .append("building", "1480")
//		                        .append("coord", asList(-73.9557413, 40.7720266)))
//		                .append("borough", "Manhattan")
//		                .append("cuisine", "Italian")
//		                .append("grades", asList(
//		                        new Document()
//		                                .append("date", format.parse("2014-10-01T00:00:00Z"))
//		                                .append("grade", "A")
//		                                .append("score", 11),
//		                        new Document()
//		                                .append("date", format.parse("2014-01-16T00:00:00Z"))
//		                                .append("grade", "B")
//		                                .append("score", 17)))
//		                .append("name", "Vella")
//		                .append("restaurant_id", "41704620"));
//		
//		FindIterable<Document> iterable = db.getCollection("restaurants").find(
//		        new Document("restaurant_id", "41704620"));
		
//		PrintHelper.printIterable(db.getCollection("restaurants").find(new Document("address.zipcode", "10016").append("cuisine", "Italian")));
		PrintHelper.printIterable(db.getCollection("restaurants").find(eq("borough", "Manhattan")).limit(1));
//		PrintHelper.printIterable(db.getCollection("restaurants").find(eq("address.zipcode", "10075")).limit(1));
//		PrintHelper.printIterable(db.getCollection("restaurants").find(eq("grades.grade", "B")).limit(1));
//		PrintHelper.printIterable(db.getCollection("restaurants").find(gt("grades.score", 30)).limit(1));
//		PrintHelper.printIterable(db.getCollection("restaurants").find(lt("grades.score", 10)).limit(1));
//		PrintHelper.printIterable(db.getCollection("restaurants").find(and(eq("cuisine", "Italian"), eq("address.zipcode", "10075"))).limit(1));
//		PrintHelper.printIterable(db.getCollection("restaurants").find(or(eq("cuisine", "Italian"), eq("address.zipcode", "10075"))).limit(100));
//		PrintHelper.printIterable(db.getCollection("restaurants").find().sort(ascending("borough", "address.zipcode")).limit(1000));
		
//		UpdateResult updateResult = db.getCollection("restaurants").updateOne(new Document("name", "Juni"),
//		        new Document("$set", new Document("cuisine", "Mexican (New)"))
//		            .append("$currentDate", new Document("lastModified", true)));
		
//		UpdateResult updateResult = db.getCollection("restaurants").updateOne(new Document("restaurant_id", "41156888"),
//		        new Document("$set", new Document("address.street", "East 31st Street")));
//		
//		PrintHelper.println(updateResult.toString());
//		PrintHelper.printIterable(db.getCollection("restaurants").find(eq("restaurant_id", "41156888")));
//		
//		UpdateResult updateManyResult = db.getCollection("restaurants").updateMany(new Document("address.zipcode", "10016").append("cuisine", "Italian"),
//        new Document("$set", new Document("cuisine", "Dutch"))
//                .append("$currentDate", new Document("lastModified", true)));
//		PrintHelper.println(updateManyResult.toString());
//		PrintHelper.printIterable(db.getCollection("restaurants").find(and(eq("cuisine", "Dutch"), eq("address.zipcode", "10016"))));

//		PrintHelper.printIterable(db.getCollection("restaurants").find(new Document("restaurant_id", "41704620")));
//		UpdateResult replaceResult = db.getCollection("restaurants").replaceOne(new Document("restaurant_id", "41704620"),
//		        new Document("address",
//		                new Document()
//		                        .append("street", "2 Avenue")
//		                        .append("zipcode", "10075")
//		                        .append("building", "1480")
//		                        .append("coord", asList(-73.9557413, 40.7720266)))
//		                .append("name", "Vella 2")
//		                .append("restaurant_id", "41704620"));
//		PrintHelper.println(replaceResult.toString());
//		PrintHelper.printIterable(db.getCollection("restaurants").find(new Document("restaurant_id", "41704620")));
//		
//		PrintHelper.printIterable(db.getCollection("restaurants").find(new Document("borough", "Manhattan")));
//		DeleteResult deleteResult = db.getCollection("restaurants").deleteMany(new Document("borough", "Manhattan")); 
//		PrintHelper.println(deleteResult.toString());
//		
//		AggregateIterable<Document> iterable = db.getCollection("restaurants").aggregate(asList(
//		        new Document("$group", new Document("_id", "$borough").append("count", new Document("$sum", 1)))));
		
		//db.getCollection("restaurants").createIndex(new Document("cuisine", 1));
		//db.getCollection("restaurants").dropIndex(new Document("borough", 1).append("address.zipcode", 1));
		
		AggregateIterable<Document> iterable = db.getCollection("restaurants").aggregate(asList(
		        new Document("$match", new Document("borough", "Queens").append("cuisine", "Brazilian")),
		        new Document("$group", new Document("_id", "$address.zipcode").append("count", new Document("$sum", 1)))));
		
		iterable.forEach(new Block<Document>() {
			@Override
		    public void apply(final Document document) {
		        System.out.println(document.toJson());
		    }
		});
		
		DBConnector.closeMongoClient(mongoClient);
    }
}
