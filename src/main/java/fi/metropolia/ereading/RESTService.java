package fi.metropolia.ereading;

import java.util.*;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

/**
 * Extends the application class of the JAX-RS service
 * @author Artem Moskalev
 *
 */
@ApplicationPath("/*")
public class RESTService extends Application {

	private Set<Class<?>> resources;
	private Set<Object> singletons;
	
	public RESTService() {
		resources = new HashSet<Class<?>>();
		singletons = new HashSet<Object>();
	}
	
	public Set<Class<?>> getClasses() {
		return resources;
	}
	public Set<Object> getSingletons() {
		return singletons;
	}	
	
}
