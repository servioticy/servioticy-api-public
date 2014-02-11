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
package es.bsc.servioticy.api;

import java.net.URI;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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


@Path("/public")
public class Paths {
  @Context UriInfo uriInfo;
  @Context
  private transient HttpServletRequest servletRequest;
  
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
  
}
