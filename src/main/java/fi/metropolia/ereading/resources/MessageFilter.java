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
	}		

}
