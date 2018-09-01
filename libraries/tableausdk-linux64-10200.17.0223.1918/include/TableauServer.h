// -----------------------------------------------------------------------
// Copyright (c) 2012 Tableau Software, Incorporated
//                    and its licensors. All rights reserved.
// Protected by U.S. Patent 7,089,266; Patents Pending.
//
// Portions of the code
// Copyright (c) 2002 The Board of Trustees of the Leland Stanford
//                    Junior University. All rights reserved.
// -----------------------------------------------------------------------
// TableauServer.h
// -----------------------------------------------------------------------
// WARNING: Computer generated file.  Do not hand modify.

#ifndef TableauServer_H
#define TableauServer_H

#include "TableauCommon.h"

#if defined(_WIN32)
#  ifdef TBL_TABLEAUSERVER_BUILD
#    define TAB_API_SERVER __declspec(dllexport)
#  else
#    define TAB_API_SERVER __declspec(dllimport)
#  endif
#else
#    define TAB_API_SERVER __attribute__ ((visibility ("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*------------------------------------------------------------------------
  SECTION
  ServerConnection

  Represents a connection to an instance of Tableau Server.

  ------------------------------------------------------------------------*/

/// Initializes a new instance of the ServerConnection class.
TAB_API_SERVER TAB_RESULT TabServerConnectionCreate(
    TAB_HANDLE *handle
);

/// Destroys a server connection object.
TAB_API_SERVER TAB_RESULT TabServerConnectionClose(TAB_HANDLE handle);

/// Sets the username and password for the HTTP proxy. This method is needed only if the server connection is going through a proxy that requires authentication.
/// @param username The username for the proxy.
/// @param password The password for the proxy.
TAB_API_SERVER TAB_RESULT TabServerConnectionSetProxyCredentials(
    TAB_HANDLE ServerConnection
    , TableauString username
    , TableauString password
);

/// Connects to the specified server and site.
/// @param host The URL of the server to connect to.
/// @param username The username of the user to sign in as. The user must have permissions to publish to the specified site.
/// @param password The password of the user to sign in as.
/// @param siteID The site ID. Pass an empty string to connect to the default site.
TAB_API_SERVER TAB_RESULT TabServerConnectionConnect(
    TAB_HANDLE ServerConnection
    , TableauString host
    , TableauString username
    , TableauString password
    , TableauString siteID
);

/// Publishes a data extract to the server.
/// @param path The path to the ".tde" file to publish.
/// @param projectName The name of the project to publish the extract to.
/// @param datasourceName The name of the data source to create on the server.
/// @param overwrite True to overwrite an existing data source on the server that has the same name; otherwise, false.
TAB_API_SERVER TAB_RESULT TabServerConnectionPublishExtract(
    TAB_HANDLE ServerConnection
    , TableauString path
    , TableauString projectName
    , TableauString datasourceName
    , int overwrite
);

/// Disconnects from the server.
TAB_API_SERVER TAB_RESULT TabServerConnectionDisconnect(
    TAB_HANDLE ServerConnection
);


/*------------------------------------------------------------------------
  SECTION
  ServerAPI

  Provides management functions for the Server API.

  ------------------------------------------------------------------------*/

/// Initializes the Server API. You must initialize the API before you call any methods in the ServerConnection class.
TAB_API_SERVER TAB_RESULT TabServerAPIInitialize(
);

/// Shuts down the Server API. You must call this method after you have finished calling other methods in the Server API.
TAB_API_SERVER TAB_RESULT TabServerAPICleanup(
);



#ifdef __cplusplus
}
#endif


#endif // TableauServer_H
