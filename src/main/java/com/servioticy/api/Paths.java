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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.servioticy.api.commons.data.Actuation;
import com.servioticy.api.commons.data.CouchBase;
import com.servioticy.api.commons.data.SO;
import com.servioticy.api.commons.data.Subscription;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.elasticsearch.SearchCriteria;
import com.servioticy.api.commons.elasticsearch.SearchEngine;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.security.IDM;
import com.servioticy.api.commons.utils.Authorization;
import com.servioticy.api.commons.utils.Config;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.queueclient.QueueClientException;

import de.passau.uni.sec.compose.pdp.servioticy.PermissionCacheObject;

@Path("/")
public class Paths {

  @Context UriInfo uriInfo;
  @Context ServletContext servletContext;
  @Context
  private transient HttpServletRequest servletRequest;


  @POST
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createSO(@Context HttpHeaders hh, String body) {

	String Acces_Token = hh.getRequestHeader(HttpHeaders.AUTHORIZATION).get(0);

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Create the Service Object
    SO so = new SO("", body); // TODO improve creation

    // requires_token false if is compose ALERT is for stream
    JsonNode security = IDM.PostSO(Acces_Token, so.getId(), true, false, false, Config.idm_host, Config.idm_port);
    if (security == null)
    	throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");

    so.appendSecurity(security);

    // Store in Couchbase
    CouchBase.setSO(so);

    // Construct the response uri
    UriBuilder ub = uriInfo.getAbsolutePathBuilder();
    URI soUri = ub.path(so.getId()).build();

    return Response.created(soUri)
             .entity(so.responseCreateSO())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  // TODO to solve with security
  @GET
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getAllSOs(@Context HttpHeaders hh) {

    String userId = (String) this.servletRequest.getAttribute("userId");

    String sos = CouchBase.getAllSOs(userId);

    return Response.ok(sos)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}")
  @GET
  @Produces("application/json")
  public Response getSO(@Context HttpHeaders hh, @PathParam("soId") String soId) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    return Response.ok(so.responseGetSO())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}")
  @PUT
  @Produces("application/json")
  public Response putSO(@Context HttpHeaders hh, @PathParam("soId") String soId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Update the Service Object
    so.update(body);

    // Store in Couchbase
    CouchBase.setSO(so);

    // Construct the response uri
    UriBuilder ub = uriInfo.getAbsolutePathBuilder();
    URI soUri = ub.path(so.getId()).build();

    return Response.ok(soUri)
             .entity(so.responseUpdateSO())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}")
  @DELETE
  @Produces("application/json")
  public Response deleteSO(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Delete all soId's updates
    List<String> ids = SearchEngine.getAllUpdatesId(soId, streamId);
    for (String id : ids)
        CouchBase.deleteData(id);

    // Delete all the subscriptions that have soId as source or destination
    ids = SearchEngine.getAllSubscriptionsBySrcAndDst(soId);
    for (String id : ids)
        CouchBase.deleteData(id);

    CouchBase.deleteSO(soId);

    return Response.noContent()
    .header("Server", "api.servIoTicy")
    .header("Date", new Date(System.currentTimeMillis()))
    .build();

  }


  @Path("/{soId}/streams")
  @GET
  @Produces("application/json")
  public Response getStreams(@Context HttpHeaders hh, @PathParam("soId") String soId) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Generate response
    String response = so.responseStreams();

    if (response == null)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(response)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/streams/{streamId}")
  @DELETE
  @Produces("application/json")
  public Response deleteAllSOData(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    List<String> ids = SearchEngine.getAllUpdatesId(soId, streamId);
    for (String id : ids)
        CouchBase.deleteData(id);

    return Response.noContent()
    .header("Server", "api.servIoTicy")
    .header("Date", new Date(System.currentTimeMillis()))
    .build();

  }


  @Path("/{soId}/streams/{streamId}")
  @PUT
  @Produces("application/json")
  public Response putSOData(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Create Data
    Data data = new Data(so, streamId, body);

    // Generate opId
    String opId = UUID.randomUUID().toString().replaceAll("-", "");

    // Create the response
    String response = body;

    // Queueing
    QueueClient sqc;
    try {
      sqc = QueueClient.factory("default.xml");
      sqc.connect();
      boolean res = sqc.put("{\"opid\": \"" + opId + "\", \"soid\": \"" + soId +
          "\", \"streamid\": \"" + streamId + "\", \"su\": " + body + "}");
      if (!res) {
        response = "{ \"message\" : \"Stored but not queued\" }";
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
    data.appendSecurity(null); // TODO to security metadata
    CouchBase.setData(data);

    // Set the opId
    CouchBase.setOpId(opId, Config.getOpIdExpiration());

//    return Response.ok(body)
    return Response.status(Response.Status.ACCEPTED)
             .entity(response)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/streams/{streamId}")
  @GET
  @Produces("application/json")
  public Response getSOData(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

//    // check authorization -> same user and not public
//    aut.checkAuthorization(so);

//    // Get the Service Object Data
    List<String> IDs = SearchEngine.getAllUpdatesId(so.getId(), streamId);
    List<Data> dataItems = new ArrayList<Data>();

    PermissionCacheObject pco = new PermissionCacheObject();
    Data su;
    for(String id : IDs) {
    	su = CouchBase.getData(id);
    	pco = aut.checkAuthorizationGetData(so, su.getSecurity(), pco);
        dataItems.add(su);
    }


    if (dataItems == null || dataItems.size() == 0)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    // Generate response
    String response = Data.responseAllData(dataItems);

    if (response == null)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(response)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/streams/{streamId}/lastUpdate")
  @GET
  @Produces("application/json")
  public Response getLastUpdate(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Get the Service Object Data
    long lastUpdate = SearchEngine.getLastUpdateTimeStamp(soId,streamId);
    Data data = CouchBase.getData(soId,streamId,lastUpdate);


    if (data == null)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(data.responseLastUpdate())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }


  @Path("/{soId}/streams/{streamId}/search")
  @POST
  @Produces("application/json")
  public Response searchUpdates(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);


    SearchCriteria filter = SearchCriteria.buildFromJson(body);

//  // Get the Service Object Data
    List<String> IDs = SearchEngine.searchUpdates(soId, streamId, filter);
    List<Data> dataItems = new ArrayList<Data>();

    for(String id : IDs)
        dataItems.add(CouchBase.getData(id));

    if (dataItems == null || dataItems.size() == 0)
        return Response.noContent()
               .header("Server", "api.servIoTicy")
               .header("Date", new Date(System.currentTimeMillis()))
               .build();

      // Generate response
      String response = Data.responseAllData(dataItems);


    if (response == null)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(response)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/streams/{streamId}/subscriptions")
  @POST
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createSubscription(@Context HttpHeaders hh, @PathParam("soId") String soId ,
                    @PathParam("streamId") String streamId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Create Subscription
    Subscription subs = new Subscription(so, streamId, body);

    // Store in Couchbase
    CouchBase.setSubscription(subs);

    // Construct the access subscription URI
    UriBuilder ub = uriInfo.getAbsolutePathBuilder();
    URI subsUri = ub.path(subs.getId()).build();

    return Response.created(subsUri)
             .entity(subs.responseCreateSubs())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/streams/{streamId}/subscriptions")
  @GET
  @Produces("application/json")
  public Response getSubscriptions(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Generate response
    String response = so.responseSubscriptions(streamId, true);

    if (response == null)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(response)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/subscriptions/{subsId}")
  @DELETE
  @Produces("application/json")
  public Response deleteSubscription(@Context HttpHeaders hh,
		  			@PathParam("subsId") String subsId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    Subscription subs = CouchBase.getSubscription(subsId);
    if (subs == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Subscription was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(subs.getSO()); // TODO check owner, only delete if is the owner

    CouchBase.deleteSubscription(subs.getKey());

    return Response.noContent()
    .header("Server", "api.servIoTicy")
    .header("Date", new Date(System.currentTimeMillis()))
    .build();

  }

  @Path("/subscriptions/{subsId}")
  @GET
  @Produces("application/json")
  public Response getSubscription(@Context HttpHeaders hh,
		  			@PathParam("subsId") String subsId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    Subscription subs = CouchBase.getSubscription(subsId);
    if (subs == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Subscription was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(subs.getSO()); // TODO check owner, only delete if is the owner

    
    return Response.ok(subs.getString())
    .header("Server", "api.servIoTicy")
    .header("Date", new Date(System.currentTimeMillis()))
    .build();

  }
  
  
  @Path("/{soId}/actuations")
  @GET
  @Produces("application/json")
  public Response getActuations(@Context HttpHeaders hh, @PathParam("soId") String soId) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);


    return Response.ok(so.getActuationsString())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/actuations/{actionId}")
  @GET
  @Produces("application/json")
  public Response getActuationStatus(@Context HttpHeaders hh, @PathParam("soId") String soId,
		  						@PathParam("actionId") String actionId) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    Actuation act = CouchBase.getActuation(actionId);

    return Response.ok(act.toString())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }


  @Path("/{soId}/actuations/{actuationName}")
  @POST
  @Produces("application/json")
  //@Consumes(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  public Response launchActuation(@Context HttpHeaders hh, @PathParam("soId") String soId,
		  @PathParam("actuationName") String actuationName, String body) {

	  Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

	  // Get the Service Object
	  SO so = CouchBase.getSO(soId);
	  if (so == null)
		  throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

	  // check authorization -> same user and not public
	  aut.checkAuthorization(so);
	  //TODO: check ownership?

	  Actuation act = new Actuation(so, actuationName, body);

	  // Generate opId
	  String opId = UUID.randomUUID().toString().replaceAll("-", "");

	  String response;
	  // Queueing
	  QueueClient sqc; //soid, streamid, body
	  try {
		  sqc = QueueClient.factory("defaultActions.xml");
		  sqc.connect();
		  System.out.println("Sending to kestrel... : "+
				  "{\"soid\": \"" + soId +
				  "\", \"id\": \"" + act.getId() +
				  "\", \"name\": \"" + actuationName +
				  "\", \"action\": " + act.toString()+ "}");

		  boolean res = sqc.put(
				  "{\"soid\": \"" + soId +
				  "\", \"id\": \"" + act.getId() +
				  "\", \"name\": \"" + actuationName +
				  "\", \"action\": " + act.toString() + "}");

		  if (res) {
			  response = "{ \"message\" : \"Actuation submitted\", " +
			  "\"id\" : \""+act.getId()+
			  "\"  }";
		  } else {
			  throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
					  "Undefined error in SQueueClient ");
		  }

		  sqc.disconnect();

	  } catch (QueueClientException e) {
		  System.out.println("Found exception: "+e+"\nmessage: "+e.getMessage());
		  throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
				  "SQueueClientException " + e.getMessage());
	  } catch (Exception e) {
		  throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
				  "Undefined error in SQueueClient");
	  }


	  // Store in Couchbase for status tracking
	  CouchBase.setActuation(act);

	  // Set the opId
	  CouchBase.setOpId(opId, Config.getOpIdExpiration());


	  // Construct the access subscription URI
	  UriBuilder ub = uriInfo.getAbsolutePathBuilder();
	  URI actUri = ub.path("../"+act.getId()).build();

	  return Response.created(actUri)
	  .entity(response)
	  .header("Server", "api.servIoTicy")
	  .header("Date", new Date(System.currentTimeMillis()))
	  .build();


  }

  @Path("/{soId}/actuations/{actuationId}")
  @PUT
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response updateActuation(@Context HttpHeaders hh, @PathParam("soId") String soId,
		  @PathParam("actuationId") String actuationId, String body) {

	  Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

	  // Check if exists request data
	  if (body.isEmpty())
		  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

	  // Get the Service Object

	  SO so = CouchBase.getSO(soId);
	  if (so == null)
		  throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

	  // check authorization -> same user and not public
	  aut.checkAuthorization(so);
	  //TODO: check ownership?

	  // Store again in Couchbase for status tracking
	  Actuation act = CouchBase.getActuation(actuationId);

	  act.updateStatus(body);

	  // Store again in Couchbase for status tracking
	  CouchBase.setActuation(act);

	  // Construct the access subscription URI
	  UriBuilder ub = uriInfo.getAbsolutePathBuilder();
	  URI actUri = ub.path(act.getId()).build();

	  return Response.created(actUri)
	  .entity(act.getStatus())
	  .header("Server", "api.servIoTicy")
	  .header("Date", new Date(System.currentTimeMillis()))
	  .build();
  }


}
