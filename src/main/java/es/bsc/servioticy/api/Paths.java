package es.bsc.servioticy.api;

import java.net.URI;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import es.bsc.servioticy.api_commons.data.CouchBase;
import es.bsc.servioticy.api_commons.data.SO;
import es.bsc.servioticy.api_commons.exceptions.ServIoTWebApplicationException;
import es.bsc.servioticy.api_commons.utils.Authorization;


@Path("/public")
public class Paths {
  @Context UriInfo uriInfo;
  @Context
  private transient HttpServletRequest servletRequest;
  
  @GET
  @Path("/hola")
  @Produces("text/plain")
  public String getHello(@Context HttpHeaders hh) {
    
//    // Check Authorization header
//    Authorization aut = new Authorization();
//    aut.checkAuthorization(hh.getRequestHeaders());

    return "Holaaaaaa";
  }

  @POST
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createThng(@Context HttpHeaders hh, String body) {
    
    String user_id = (String) this.servletRequest.getAttribute("user_id");
    
    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");
    
    // Create the Service Object
    SO so = new SO(user_id, body);
    
    // Store in Couchbase
    CouchBase cb = new CouchBase();
    cb.setSO(so);
    
    // Construct the access thng URI
    String thngUri = uriInfo.getAbsolutePath().toString();
    thngUri = thngUri.substring(0, thngUri.lastIndexOf('/')); //Uri without .../create.json
    UriBuilder ub = UriBuilder.fromPath(thngUri);
    URI userUri = ub.path(so.getId())
            .build();
    
    return Response.created(userUri)
             .entity(so.getString())
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }


}
