// -----------------------------------------------------------------------
// Copyright (c) 2012 Tableau Software, Incorporated
//                    and its licensors. All rights reserved.
// Protected by U.S. Patent 7,089,266; Patents Pending.
//
// Portions of the code
// Copyright (c) 2002 The Board of Trustees of the Leland Stanford
//                    Junior University. All rights reserved.
// -----------------------------------------------------------------------
// TableauServer_cpp.h
// -----------------------------------------------------------------------
// WARNING: Computer generated file.  Do not hand modify.

#ifndef TableauServer_CPP_H
#define TableauServer_CPP_H

#include "TableauServer.h"
#include "TableauCommon_cpp.h"
#include <string>

namespace Tableau {

typedef void* TableauHandle;



} // namespace Tableau

#ifdef __GNUC__
#  if __GNUC__ < 4 || (__GNUC__ == 4 && __GNUC_MINOR__ < 6)
#    define nullptr NULL
#  endif
#endif

namespace Tableau {

/*------------------------------------------------------------------------
  CLASS
  ServerConnection

  Represents a connection to an instance of Tableau Server.

  ------------------------------------------------------------------------*/

class ServerConnection
{
  public:
    /// Initializes a new instance of the ServerConnection class.
    ServerConnection(
    );

    /// Destroys a server connection object.
    void Close();

    /// Calls Close().
    ~ServerConnection();

    /// Sets the username and password for the HTTP proxy. This method is needed only if the server connection is going through a proxy that requires authentication.
    /// @param username The username for the proxy.
    /// @param password The password for the proxy.
    void
    SetProxyCredentials(
        std::wstring username,
        std::wstring password
    );

    /// Connects to the specified server and site.
    /// @param host The URL of the server to connect to.
    /// @param username The username of the user to sign in as. The user must have permissions to publish to the specified site.
    /// @param password The password of the user to sign in as.
    /// @param siteID The site ID. Pass an empty string to connect to the default site.
    void
    Connect(
        std::wstring host,
        std::wstring username,
        std::wstring password,
        std::wstring siteID
    );

    /// Publishes a data extract to the server.
    /// @param path The path to the ".tde" file to publish.
    /// @param projectName The name of the project to publish the extract to.
    /// @param datasourceName The name of the data source to create on the server.
    /// @param overwrite True to overwrite an existing data source on the server that has the same name; otherwise, false.
    void
    PublishExtract(
        std::wstring path,
        std::wstring projectName,
        std::wstring datasourceName,
        bool overwrite
    );

    /// Disconnects from the server.
    void
    Disconnect(
    );


  private:
    TAB_HANDLE m_handle;

    // Forbidden:
    ServerConnection( const ServerConnection& );
    ServerConnection& operator=( const ServerConnection& );

};

/*------------------------------------------------------------------------
  CLASS
  ServerAPI

  Provides management functions for the Server API.

  ------------------------------------------------------------------------*/

class ServerAPI
{
  public:
    /// Initializes the Server API. You must initialize the API before you call any methods in the ServerConnection class.
    static
    void
    Initialize(
    );

    /// Shuts down the Server API. You must call this method after you have finished calling other methods in the Server API.
    static
    void
    Cleanup(
    );


  private:
    // Forbidden:
    ServerAPI( const ServerAPI& );
    ServerAPI& operator=( const ServerAPI& );

};




// -----------------------------------------------------------------------
// ServerConnection methods
// -----------------------------------------------------------------------

// Initializes a new instance of the ServerConnection class.
inline ServerConnection::ServerConnection(
)
{
    TAB_RESULT result = TabServerConnectionCreate(
        &m_handle
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Destroys a server connection object.
inline ServerConnection::~ServerConnection()
{
    Close();
}

inline void ServerConnection::Close()
{
    if ( m_handle != nullptr ) {
        TAB_RESULT result = TabServerConnectionClose( m_handle );
        m_handle = nullptr;

        if ( result != TAB_RESULT_Success )
            throw TableauException( result, TabGetLastErrorMessage() );
    }
}

// Sets the username and password for the HTTP proxy. This method is needed only if the server connection is going through a proxy that requires authentication.
inline void
ServerConnection::SetProxyCredentials(
    std::wstring username,
    std::wstring password
)
{
    TAB_RESULT result = TabServerConnectionSetProxyCredentials(m_handle
        , MakeTableauString(username.c_str()).c_str()
        , MakeTableauString(password.c_str()).c_str()
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Connects to the specified server and site.
inline void
ServerConnection::Connect(
    std::wstring host,
    std::wstring username,
    std::wstring password,
    std::wstring siteID
)
{
    TAB_RESULT result = TabServerConnectionConnect(m_handle
        , MakeTableauString(host.c_str()).c_str()
        , MakeTableauString(username.c_str()).c_str()
        , MakeTableauString(password.c_str()).c_str()
        , MakeTableauString(siteID.c_str()).c_str()
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Publishes a data extract to the server.
inline void
ServerConnection::PublishExtract(
    std::wstring path,
    std::wstring projectName,
    std::wstring datasourceName,
    bool overwrite
)
{
    TAB_RESULT result = TabServerConnectionPublishExtract(m_handle
        , MakeTableauString(path.c_str()).c_str()
        , MakeTableauString(projectName.c_str()).c_str()
        , MakeTableauString(datasourceName.c_str()).c_str()
        , overwrite
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Disconnects from the server.
inline void
ServerConnection::Disconnect(
)
{
    TAB_RESULT result = TabServerConnectionDisconnect(m_handle
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}



// -----------------------------------------------------------------------
// ServerAPI methods
// -----------------------------------------------------------------------

// Initializes the Server API. You must initialize the API before you call any methods in the ServerConnection class.
inline void
ServerAPI::Initialize(
)
{
    TAB_RESULT result = TabServerAPIInitialize(
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Shuts down the Server API. You must call this method after you have finished calling other methods in the Server API.
inline void
ServerAPI::Cleanup(
)
{
    TAB_RESULT result = TabServerAPICleanup(
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

} // namespace Tableau
#endif // TableauServer_CPP_H
