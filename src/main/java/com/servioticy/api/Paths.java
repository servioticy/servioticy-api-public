/*******************************************************************************
 * Copyright 2014 Barcelona Supercomputing Center (BSC)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.servioticy.api;

import java.net.URI;
import java.util.Date;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.servioticy.api.commons.data.CouchBase;
import com.servioticy.api.commons.data.Group;
import com.servioticy.api.commons.data.SO;
import com.servioticy.api.commons.data.Subscription;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Config;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.queueclient.QueueClientException;


@Path("/public")
public class Paths {
  @Context UriInfo uriInfo;
  @Context
  private transient HttpServletRequest servletRequest;
  
  
  @GET
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getAllSOs(@Context HttpHeaders hh) {
    
    String user_id = (String) this.servletRequest.getAttribute("user_id");
    
    CouchBase cb = new CouchBase();
    String sos = cb.getAllSOs(user_id);
    
    return Response.ok(sos)
           .header("Server", "api.compose")
           .header("Date", new Date(System.currentTimeMillis()))
           .build();
  }  
  
  @POST
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createSO(@Context HttpHeaders hh, String body) {
    
    String user_id = (String) this.servletRequest.getAttribute("user_id");
    
    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");
    
    // Create the Service Object
    SO so = new SO(user_id, body);
    
    // Store in Couchbase
    CouchBase cb = new CouchBase();
    cb.setSO(so);
    
    // Construct the response uri
    UriBuilder ub = uriInfo.getAbsolutePathBuilder();
    URI soUri = ub.path(so.getId()).build();
    
    return Response.created(soUri)
             .entity(so.getString())
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}")
  @GET
  @Produces("application/json")
  public Response getSO(@Context HttpHeaders hh, @PathParam("soId") String so_id) {

    String user_id = (String) this.servletRequest.getAttribute("user_id");
    
    // Get the Service Object
    CouchBase cb = new CouchBase();
    SO storedSO = cb.getSO(user_id, so_id);
    if (storedSO == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");
    
    return Response.ok(storedSO.getString())
           .header("Server", "api.compose")
           .header("Date", new Date(System.currentTimeMillis()))
           .build();
  }

  @Path("/{soId}/streams/{streamId}/subscriptions")
  @POST
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createSubscription(@Context HttpHeaders hh, @PathParam("soId") String soId , 
                    @PathParam("streamId") String streamId, String body) {

    String user_id = (String) this.servletRequest.getAttribute("user_id");
    
    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");
    
    // Create Subscription
    Subscription subs = new Subscription(user_id, soId, streamId, body);
    
    // Store in Couchbase
    CouchBase cb = new CouchBase();
    cb.setSubscription(subs);
    
    // Construct the access subscription URI
    UriBuilder ub = uriInfo.getAbsolutePathBuilder();
    URI subsUri = ub.path(subs.getId()).build();

    return Response.created(subsUri)
             .entity(subs.getString())
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  } 

	@Path("/{soId}/streams/{streamId}/subscriptions")
	@GET
	@Produces("application/json")
	public Response getSubscriptions(@Context HttpHeaders hh, @PathParam("soId") String soId,
										@PathParam("streamId") String streamId) {

    String user_id = (String) this.servletRequest.getAttribute("user_id");
		
		// Get the Service Object
		CouchBase cb = new CouchBase();
		SO so = cb.getSO(user_id, soId);
		if (so == null)
			throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");
		
		return Response.ok(so.responseSubscriptions(streamId))
					   .header("Server", "api.compose")
					   .header("Date", new Date(System.currentTimeMillis()))
					   .build();
	}

  @Path("/{soId}/streams/{streamId}")
  @PUT
  @Produces("application/json")
  public Response updateSOData(@Context HttpHeaders hh, @PathParam("soId") String soId, 
                    @PathParam("streamId") String streamId, String body) {

    String user_id = (String) this.servletRequest.getAttribute("user_id");
		
    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");
    
    // Create Data
    Data data = new Data(user_id, soId, streamId, body);

    // Generate opId
    String opId = UUID.randomUUID().toString().replaceAll("-", "");

    // Queueing
    QueueClient sqc;
    try {
      sqc = QueueClient.factory();
      sqc.connect();
      boolean res = sqc.put("{\"opid\": " + opId + ", \"soid\": " + soId + 
          ", \"streamid\": " + streamId + ", \"su\": " + body);
      if (!res) {
        // TODO -> what to do with res = false
      }
      sqc.disconnect();

    } catch (QueueClientException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, 
          "SQueueClientException " + e.getMessage());
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, 
          "Undefined error in SQueueClient");
    }
    
    // Store in Couchbase
		CouchBase cb = new CouchBase();
    cb.setData(data);
    
    // Set the opId
    cb.setOpId(opId, Config.getOpIdExpiration());
    
    return Response.ok(body)
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }
  
  @Path("/{soId}/streams/{streamId}/lastUpdate")
  @GET
  @Produces("application/json")
  public Response getLastUpdate(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId) {

    String user_id = (String) this.servletRequest.getAttribute("user_id");
    
    // Get the Service Object
    CouchBase cb = new CouchBase();
    Data data = cb.getData(user_id, soId, streamId);

    if (data == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "No data in the stream of the Service Object.");
    
    return Response.ok(data.lastUpdate().toString())
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }
  
  @Path("/groups/lastUpdate")
  @POST
  @Produces("application/json")
  public Response getLastGroupUpdate(@Context HttpHeaders hh, String body) {

    String user_id = (String) this.servletRequest.getAttribute("user_id");
    
    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");
    
    // Create Group petition
    Group group = new Group(user_id, body);
    
//    if (group == null)
//      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "No data in the stream of the Service Object.");
    
    return Response.ok(group.lastUpdate().toString())
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }
}
