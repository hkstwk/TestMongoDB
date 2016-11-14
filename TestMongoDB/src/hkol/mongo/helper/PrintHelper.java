package hkol.mongo.helper;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;

public class PrintHelper {
	public static void printIterable(FindIterable<Document> iterable){
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        println(document.toJson());
		    }
		});
	}
	
	public static void println(String str){
		System.out.println(str);
	}
}