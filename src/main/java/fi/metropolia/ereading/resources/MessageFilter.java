package fi.metropolia.ereading.resources;

import org.editeur.ns.onix._3_0.reference.*;

public class MessageFilter {

	public static void filter(ONIXMessage message) throws Exception {
		if (message.getHeader().getSender() == null) {
			throw new Exception("Sender must be specified for the message!");
		}		 
		if(message.getProduct().size() == 0) {
			throw new Exception("At least one Product must be specified!");
		}
		if(message.getHeader().getSentDateTime() == null) {
			throw new Exception("SentDateTime is not specified for the message!");
		}		
		if (message.getProduct().size() == 0) {
			throw new Exception("At least one product must be in the set!");
		}
		if (message.getProduct().size() == 0) {
			throw new Exception("At least one product must be in the set!");
		}
		for (Product product : message.getProduct()) {
			if(product.getRecordReference() == null) {
				throw new Exception("Every product should have Record Reference!");
			}	
			if(product.getNotificationType() == null) {
				throw new Exception("Every product should have Notification Type!");
			}
			if(product.getProductIdentifier().size() == 0) {
				throw new Exception("Every product should have at least one Product Identifier!");
			}			
			if(!product.getNotificationType().getValue().equals("03") && !product.getNotificationType().getValue().equals("02") 
					&& !product.getNotificationType().getValue().equals("01") && !product.getNotificationType().getValue().equals("04")
					&& !product.getNotificationType().getValue().equals("05")) {
				throw new Exception("Notification Type code is not supported!");
			}
		}
		
	}		

}
