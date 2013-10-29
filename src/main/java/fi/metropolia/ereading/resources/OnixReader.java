package fi.metropolia.ereading.resources;

import java.io.*;
import java.util.*;

import javax.annotation.ManagedBean;
import javax.inject.Inject;
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

import fi.metropolia.ereading.background.BusFactory;
import fi.metropolia.ereading.background.Outlet;

@Path("/")
@ManagedBean
public class OnixReader {
	
	private static MongoClient mongo;
	private static JAXBContext jsonContext;
	@Inject
	private BusFactory factory;
	
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
	public Response submitONIX(ONIXMessage message, @QueryParam("key") String key) {	
		/* exits if the message is sent without any authorization keys */
		if (key == null || mongo.getDB("jOnix").getCollection("users").getCount(new BasicDBObject("key", key)) == 0) {
			return Response.status(Status.UNAUTHORIZED).build();
		} 				
		/* filters the messages that are not properly made */		
		try {
			MessageFilter.filter(message);			
		} catch(Exception ex) {
			return Response.status(Status.NOT_ACCEPTABLE).entity(ex.getMessage()).build();
		}
		DBCollection headersCollection = mongo.getDB("jOnix").getCollection("headers");
		DBCollection productsCollection = mongo.getDB("jOnix").getCollection("products");
		
		ObjectId id = new ObjectId();
		
		Header header = message.getHeader();
		List<Product> products = message.getProduct();
		try {						
			Marshaller marshaller = getMarshaller();
			/* this chunk of code resends the message to the output busses of addressees */
			for (Addressee addressee : message.getHeader().getAddressee()) {
				for(java.lang.Object temp : addressee.getContent()) {
					if(temp instanceof AddresseeName) {
						String organization = ((AddresseeName) temp).getValue();
						Outlet bus = factory.getBusIfExists(organization);
						if (bus != null) {
							try {
								bus.sendOnixMessage(message, marshaller);
							} catch (Exception e) {
								e.printStackTrace();
							}
							break;
						}
					}
				}
			}
			/* parses the Notification Type and stores the message in the database */
			StringWriter headerWriter = new StringWriter();
			marshaller.marshal(header, headerWriter);
			headersCollection.insert(new BasicDBObject("item", JSON.parse(headerWriter.toString())).append("_id", id));
			
			for (Product product : products) {
				if (product.getNotificationType().getValue().equals("05")) {
					DBCursor cursor = productsCollection.find(new BasicDBObject("item.Product.RecordReference", 
															  product.getRecordReference().getValue()));
					while(cursor.hasNext()) {
						DBObject temp = cursor.next();						
						DBCursor toRemoveCursor = productsCollection.find(new BasicDBObject("refer", temp.get("refer")));
						if (toRemoveCursor.size() == 1) {
							productsCollection.remove(temp);
							headersCollection.remove(new BasicDBObject("_id", temp.get("refer")));
						} else {
							productsCollection.remove(temp);
						}
					}	
					// removes the entry which has just bee ncreated by the same query
					headersCollection.remove(new BasicDBObject("_id", id)); 
					
				} else if (product.getNotificationType().getValue().equals("01") || product.getNotificationType().getValue().equals("02")
						|| product.getNotificationType().getValue().equals("03")) {
					int newVersion = 0;
					DBCursor version = productsCollection.find(new BasicDBObject("item.Product.RecordReference", 
																product.getRecordReference().getValue()))
														 .sort(new BasicDBObject("VERSION", -1)).limit(1);
					if(version.size() > 0) {
						newVersion = Integer.valueOf(JSON.serialize(version.next().get("VERSION"))) + 1;
					} else {
						newVersion = 0;
					}
					StringWriter productWriter = new StringWriter();			
					marshaller.marshal(product, productWriter);
					productsCollection.insert(new BasicDBObject("item", JSON.parse(productWriter.toString()))
												.append("VERSION", newVersion).append("refer", id));
				} else if (product.getNotificationType().getValue().equals("04")) {
					
					int newVersion = 0;
					DBObject updatedProduct = null;
					
					DBCursor version = productsCollection.find(new BasicDBObject("item.Product.RecordReference", 
																product.getRecordReference().getValue()))
														 .sort(new BasicDBObject("VERSION", -1)).limit(1);
					if(version.size() > 0) {
						updatedProduct = version.next();
						newVersion = Integer.valueOf(JSON.serialize(updatedProduct.get("VERSION"))) + 1;
					} else {
						newVersion = 0;
					}	
					
					BasicDBObject insertedUpdate = new BasicDBObject("item", updatedProduct.get("item")).append("VERSION", newVersion)
													.append("refer", id);
					productsCollection.insert(insertedUpdate);
					
					if(product.getDescriptiveDetail() != null) {
						StringWriter productWriter = new StringWriter();
						marshaller.marshal(product.getDescriptiveDetail(), productWriter);
						productsCollection.update(new BasicDBObject("_id", id), 
												  new BasicDBObject("$set", 
														  			new BasicDBObject("item.Product.DescriptiveDetail", JSON.parse(productWriter.toString()))));
					}
					if(product.getCollateralDetail() != null) {
						StringWriter productWriter = new StringWriter();
						marshaller.marshal(product.getCollateralDetail(), productWriter);
						productsCollection.update(new BasicDBObject("_id", id), 
								  new BasicDBObject("$set", 
										  			new BasicDBObject("item.Product.CollateralDetail", JSON.parse(productWriter.toString()))));
					}
					if(product.getContentDetail() != null) {
						StringWriter productWriter = new StringWriter();
						marshaller.marshal(product.getContentDetail(), productWriter);
						productsCollection.update(new BasicDBObject("_id", id), 
								  new BasicDBObject("$set", 
										  			new BasicDBObject("item.Product.ContentDetail", JSON.parse(productWriter.toString()))));
					}
					if(product.getPublishingDetail() != null) {
						StringWriter productWriter = new StringWriter();
						marshaller.marshal(product.getPublishingDetail(), productWriter);
						productsCollection.update(new BasicDBObject("_id", id), 
								  new BasicDBObject("$set", 
										  			new BasicDBObject("item.Product.PublishingDetail", JSON.parse(productWriter.toString()))));
					}
					if(product.getRelatedMaterial() != null) {
						StringWriter productWriter = new StringWriter();
						marshaller.marshal(product.getRelatedMaterial(), productWriter);
						productsCollection.update(new BasicDBObject("_id", id), 
								  new BasicDBObject("$set", 
										  			new BasicDBObject("item.Product.RelatedMaterial", JSON.parse(productWriter.toString()))));
					}
					if(product.getProductSupply() != null) {
						StringWriter productWriter = new StringWriter();
						marshaller.marshal(product.getProductSupply(), productWriter);
						productsCollection.update(new BasicDBObject("_id", id), 
								  new BasicDBObject("$set", 
										  			new BasicDBObject("item.Product.ProductSupply", JSON.parse(productWriter.toString()))));
					}						
				} 				
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
	
	@GET
	@Path("products/{reference}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public Response getProductByReference(@PathParam("reference") String reference) {
		DBCollection productsCollection = mongo.getDB("jOnix").getCollection("products");
		
		DBCursor productRecord = productsCollection.find(new BasicDBObject("item.Product.RecordReference", reference))
												   .sort(new BasicDBObject("VERSION", -1)).limit(1);
		if (productRecord.hasNext()) {
			Product product = null;
			try {
				product = (Product)getUnmarshaller().unmarshal(new StringReader(JSON.serialize(productRecord.next().get("item"))));
			} catch (Exception e) {
				e.printStackTrace();
			}
			return Response.ok(product).build();
		} else {
			return Response.status(Status.NOT_FOUND).build();
		}
	}
	@GET
	@Path("products/{reference}/history")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public Response getProductByReferenceWithHistory(@PathParam("reference") String reference) {
		DBCollection productsCollection = mongo.getDB("jOnix").getCollection("products");
		
		DBCursor productRecords = productsCollection.find(new BasicDBObject("item.Product.RecordReference", reference))
													.sort(new BasicDBObject("VERSION", -1));
		List<Product> products = new ArrayList<Product>();;
		if (productRecords.size() != 0) {
			while(productRecords.hasNext()) {
				DBObject productRecord = productRecords.next();
				try {
					products.add((Product)getUnmarshaller().unmarshal(new StringReader(JSON.serialize(productRecord.get("item")))));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return Response.ok(new GenericEntity<List<Product>>(products){	}).build();
		} else {
			return Response.status(Status.NOT_FOUND).build();
		}
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
