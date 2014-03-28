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

import javax.servlet.ServletContext;
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
import com.servioticy.api.commons.data.SO;
import com.servioticy.api.commons.data.Subscription;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Authorization;
import com.servioticy.api.commons.utils.Config;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.queueclient.QueueClientException;


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

    String userId = (String) this.servletRequest.getAttribute("userId");

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Create the Service Object
    SO so = new SO(userId, body);

    // Store in Couchbase
    CouchBase cb = new CouchBase();
    cb.setSO(so);

    // Construct the response uri
    UriBuilder ub = uriInfo.getAbsolutePathBuilder();
    URI soUri = ub.path(so.getId()).build();

    return Response.created(soUri)
             .entity(so.responseCreateSO())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @GET
  @Produces("application/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getAllSOs(@Context HttpHeaders hh) {

    String userId = (String) this.servletRequest.getAttribute("userId");

    CouchBase cb = new CouchBase();
    String sos = cb.getAllSOs(userId);

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
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    return Response.ok(so.responseGetSO())
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
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
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
  @PUT
  @Produces("application/json")
  public Response updateSOData(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId, String body) {

    Authorization aut = (Authorization) this.servletRequest.getAttribute("aut");

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Get the Service Object
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
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
    cb.setData(data);

    // Set the opId
    cb.setOpId(opId, Config.getOpIdExpiration());

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
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Get the Service Object Data
    Data data = cb.getData(so, streamId);

    if (data == null)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    // Generate response
    String response = data.responseAllData();

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
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Get the Service Object Data
    Data data = cb.getData(so, streamId);

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
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Create Subscription
    Subscription subs = new Subscription(so, streamId, body);

    // Store in Couchbase
    cb.setSubscription(subs);

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
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // check authorization -> same user and not public
    aut.checkAuthorization(so);

    // Generate response
    String response = so.responseSubscriptions(streamId);

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
}
