package fi.metropolia.ereading.resources;

import java.io.*;
import java.util.*;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import javax.xml.bind.*;

import org.bson.types.ObjectId;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.editeur.ns.onix._3_0.reference.*;

import com.mongodb.*;
import com.mongodb.util.JSON;

@Path("/")
public class OnixReader {
	
	private static MongoClient mongo;
	private static JAXBContext jsonContext;
	
	static {
		try {
			mongo = new MongoClient("localhost", 27017);
		} catch (Exception ex) {
			ex.printStackTrace();
		}	
		try {
			jsonContext = JAXBContextFactory.createContext(new Class[] {ONIXMessage.class}, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	@POST
	@Path("/send")
	@Consumes(MediaType.APPLICATION_XML)
	public Response submitONIX(ONIXMessage message) {			
		DBCollection headersCollection = mongo.getDB("jOnix").getCollection("headers");
		DBCollection productsCollection = mongo.getDB("jOnix").getCollection("products");
		
		try {
			MessageFilter.filter(message);			
		} catch(Exception ex) {
			return Response.status(Status.NOT_ACCEPTABLE).entity(ex.getMessage()).build();
		}
		
		ObjectId id = new ObjectId();
		
		Header header = message.getHeader();
		List<Product> products = message.getProduct();
		try {						
			Marshaller marshaller = getMarshaller();
			
			StringWriter headerWriter = new StringWriter();
			marshaller.marshal(header, headerWriter);
			headersCollection.insert(new BasicDBObject("item", JSON.parse(headerWriter.toString())).append("_id", id));
			
			for (Product product : products) {
				StringWriter productWriter = new StringWriter();			
				marshaller.marshal(product, productWriter);
				productsCollection.insert(new BasicDBObject("item", JSON.parse(productWriter.toString())).append("refer", id));
			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
				
		return Response.status(Status.ACCEPTED).build();	
	}
	
	@GET
	@Path("/messages/addressee/{name}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public Response getONIXMessageByAddressee(@PathParam("name") String addressee) {
		DBCollection headersCollection = mongo.getDB("jOnix").getCollection("headers");
		DBCollection productsCollection = mongo.getDB("jOnix").getCollection("products");

		List<ONIXMessage> messages = new LinkedList<ONIXMessage>();
		Unmarshaller unmarshaller = null;
		try {
			unmarshaller = getUnmarshaller();
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		/* specifying the query for the messages with the addressee specified */
		DBCursor cursor = null;
		if (!addressee.equals("all")) {
			BasicDBList nameList = new BasicDBList();
			nameList.put(0, addressee);
			BasicDBObject name = new BasicDBObject("$all", nameList);		
			cursor = headersCollection.find(new BasicDBObject("item.Header.Addressee.AddresseeName", name));
		} else {
			cursor = headersCollection.find();
		}
		/* iterating over all headers */
		while(cursor.hasNext()) {
			ONIXMessage message = new ONIXMessage();
			DBObject tempHeader = cursor.next();
			
			Header header = null;
			try {
				header = (Header)unmarshaller.unmarshal(new StringReader(JSON.serialize(tempHeader.get("item"))));
			} catch (JAXBException e) {
				e.printStackTrace();
			}
			message.setHeader(header);
			
			java.lang.Object key = tempHeader.get("_id");
			DBCursor productCursor = productsCollection.find(new BasicDBObject("refer", key));
			
			while(productCursor.hasNext()) {
				DBObject tempProduct = productCursor.next();
				try {
					Product product = (Product)unmarshaller.unmarshal(new StringReader(JSON.serialize(tempProduct.get("item"))));
					message.getProduct().add(product);
				} catch (JAXBException e) {
					e.printStackTrace();
				}
			}
			messages.add(message);
		}
		
		return Response.ok(new GenericEntity<List<ONIXMessage>>(messages){	}).build();
	}
	
	private Unmarshaller getUnmarshaller() throws Exception {
		Unmarshaller unmarshaller = jsonContext.createUnmarshaller();
		unmarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json");
		return unmarshaller;
	}
	private Marshaller getMarshaller() throws Exception {
		Marshaller marshaller = jsonContext.createMarshaller();
		marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json");
		return marshaller;
	}
	
}
