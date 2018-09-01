//------------------------------------------------------------------------------
//
//  This file is the copyrighted property of Tableau Software and is protected
//  by registered patents and other applicable U.S. and international laws and
//  regulations.
//
//  Unlicensed use of the contents of this file is prohibited. Please refer to
//  the NOTICES.txt file for further details.
//
//  NOTE: This sample requires a C99 or higher compiler, i.e. Microsoft Visual
//  C compiler 2013 and above, and GCC with C99.
//
//------------------------------------------------------------------------------
#if defined(__APPLE__) && defined(__MACH__)
#include <TableauExtract/TableauExtract.h>
#include <TableauServer/TableauServer.h>
#else
#include "TableauExtract.h"
#include "TableauServer.h"
#endif

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//------------------------------------------------------------------------------
//  TableauString Helpers
//------------------------------------------------------------------------------
#define CreateTableauStringConst( STR, NAME )                                  \
    static const wchar_t NAME##_str[] = STR;                                   \
    TableauWChar NAME[ sizeof( NAME##_str ) / sizeof( wchar_t ) ];             \
    ToTableauString( NAME##_str, NAME )

#if defined(_WIN32) || defined(_WIN64)
#define TO_WCHAR_FMT_STR L"%S"
#else
#define TO_WCHAR_FMT_STR L"%s"
#endif
#define CreateTableauStringOption( ARG, OPTION )                               \
    wchar_t *buf = ( wchar_t * )calloc( strlen( ARG ) + 1, sizeof( wchar_t ) );\
    OPTION = ( TableauWChar * )calloc( strlen( ARG ) + 1, sizeof( TableauWChar ) ); \
    swprintf( buf, strlen( ARG ) * sizeof( wchar_t ), TO_WCHAR_FMT_STR, ARG ); \
    ToTableauString( buf, OPTION );                                            \
    free( buf )

//------------------------------------------------------------------------------
//  Error Handling Helpers
//------------------------------------------------------------------------------
void
TryOp(
    TAB_RESULT result,
    const char *description,
    const wchar_t *message
)
{
    if ( result != TAB_RESULT_Success ) {
        fprintf( stderr,
            "A fatal error occurred in %s:\n %ls \nExiting now.\n",
             description, message );
        exit( EXIT_FAILURE );
    }
}

#define ExtractTryOp( OP )                                                     \
    TryOp( OP, __FUNCTION__, TabGetLastErrorMessage() );

void
ServerTryOp(
    TAB_RESULT result
)
{
    if ( result != TAB_RESULT_Success ) {
        const wchar_t* errorType = NULL;
        const wchar_t* message = NULL;

        if ( result == TAB_RESULT_InternalError )
        {
            errorType = L"InternalError";
            message = L"Could not parse the response from the server.";
        }
        else if ( result == TAB_RESULT_InvalidArgument ) {
            errorType = L"InvalidArgument";
            message = TabGetLastErrorMessage();
        }
        else if ( result == TAB_RESULT_CurlError ) {
            errorType = L"CurlError";
            message = TabGetLastErrorMessage();
        }
        else if ( result == TAB_RESULT_ServerError ) {
            errorType = L"ServerError";
            message = TabGetLastErrorMessage();
        }
        else if ( result == TAB_RESULT_NotAuthenticated ) {
            errorType = L"NotAuthenticated";
            message = TabGetLastErrorMessage();
        }
        else if ( result == TAB_RESULT_BadPayload ) {
            errorType = L"BadPayload";
            message = L"Unknown response from the server. Make sure this version of the Tableau SDK is compatible with your server.";
        }
        else if (result == TAB_RESULT_InitError ) {
            errorType = L"InitError";
            message = TabGetLastErrorMessage();
        }
        else {
            errorType = L"UnknownError";
            message = L"An unknown error occurred.";
        }

        fprintf( stderr,
            "A fatal error occurred while publishing the extract:\n%ls - %ls\nExiting now.n",
            errorType, message );
        exit( EXIT_FAILURE );
    }
}

//------------------------------------------------------------------------------
//  Display Usage
//------------------------------------------------------------------------------
void
DisplayUsage()
{
    fprintf( stderr,
        "usage: tableauSDKSample    [-h] [-b] [-s] [-p] [-o] [-f FILENAME]\n"
        "                           [--project-name PROJECT_NAME]\n"
        "                           [--datasource-name DATASOURCE_NAME]\n"
        "                           [--hostname HOSTNAME] [--username USERNAME]\n"
        "                           [--password PASSWORD] [--site-id SITEID]\n"
        "\n"
        "A simple demonstration of the Tableau SDK.\n"
        "\n"
        "optional arguments:\n"
        " -h, --help            show this help message and exit\n"
        " -b, --build           If an extract named FILENAME exists, open it and add data to\n"
        "                       it. If no such extract exists, create one and add data to it.\n"
        "                       If no FILENAME is specified, the default is used.\n"
        "                       (default=False)\n"
        "\n"
        " -s, --spatial         If creating a new extract, include spatial data when adding\n"
        "                       data. If '--build' is not specified, or the extract being\n"
        "                       built is not newly created, this argument is ignored.\n"
        "                       (default=False)\n"
        "\n"
        " -p, --publish         Publish an extract named FILENAME to a Tableau Server instance\n"
        "                       running at HOSTNAME, creating a published datasource named\n"
        "                       DATASOURCE_NAME on the server under the PROJECT_NAME project.\n"
        "\n"
        "                       If '--overwrite' is specified, if there is an existing\n"
        "                       published datasource on the server named DATASOURCE_NAME under\n"
        "                       the PROJECT_NAME project, it is overwritten.\n"
        "                       USERNAME, PASSWORD, and SITEID are used to connect to the\n"
        "                       server.\n"
        "\n"
        "                       If any of '--filename', '--project-name', '--datasource-name',\n"
        "                       '--overwrite', '--hostname', '--username', '--password',\n"
        "                       or '--site-id' are not specified, the corresponding default\n"
        "                       value(s) are used.\n"
        "\n"
        "                       (NOTE: If '--username', '--password', and '--site-id' are not\n"
        "                        each specified, '--publish' will not succeed.)\n"
        "                       (default=False)\n"
        "\n"
        " -o, --overwrite       Overwrite any existing published datasource named\n"
        "                       DATASOURCE_NAME under the PROJECT_NAME project on the server.\n"
        "                       If '--project-name' and/or '--datasource-name' are not\n"
        "                       specified, the corresponding default value(s) are used. If\n"
        "                       '--publish' is not specified, this argument is ignored\n"
        "                       (default=False)\n"
        "\n"
        " -f FILENAME, --filename FILENAME\n"
        "                       Use FILENAME as the extract filename when creating, opening,\n"
        "                       and/or publishing an extract. If neither '--build' nor\n"
        "                       '--publish' is specified, this argument is ignored.\n"
        "                       (default='order-c.tde')\n"
        "\n"
        " --project-name PROJECT_NAME\n"
        "                       Use PROJECT_NAME as the project-name when creating publishing\n"
        "                       an extract. If '--publish' is not specified, this argument is\n"
        "                       ignored.\n"
        "                       (default='default')\n"
        "\n"
        " --datasource-name DATASOURCE_NAME\n"
        "                       Use DATASOURCE_NAME as the datasource name when creating\n"
        "                       publishing an extract. If '--publish' is not specified, this\n"
        "                       argument is ignored.\n"
        "                       (default='order-c')\n"
        "\n"
        " --hostname HOSTNAME   Connect to a Tableau Server instance running at HOSTNAME to\n"
        "                       publish an extract. If '--publish' is not specified, this\n"
        "                       argument is ignored.\n"
        "                       (default='http://localhost')\n"
        "\n"
        " --username USERNAME   Connect to the server as user USERNAME to publish an extract.\n"
        "                       If '--publish' is not specified, this argument is ignored.\n"
        "\n"
        "                       (NOTE: This argument must be specified for '--publish' to\n"
        "                        succeed. Admin privileges are required in Tableau Server to\n"
        "                        publish datasources using the Tableau SDK Server API.)\n"
        "                       (default='username')\n"
        "\n"
        " --password PASSWORD   Connect to the server using password PASSWORD to publish an\n"
        "                       extract. If '--publish' is not specified, this argument is\n"
        "                       ignored.\n"
        "\n"
        "                       (NOTE: This argument must be specified for '--publish' to\n"
        "                        succeed. Admin privileges are required in Tableau Server to\n"
        "                        publish datasources using the Tableau SDK Server API.)\n"
        "                       (default='password')\n"
        "\n"
        " --site-id SITEID      Connect to the server using siteID SITEID to publish an\n"
        "                       extract. If '--publish' is not specified, this argument is\n"
        "                       ignored.\n"
        "\n"
        "                       (NOTE: This argument must be specified for '--publish' to\n"
        "                        succeed. Admin privileges are required in Tableau Server to\n"
        "                        publish datasources using the Tableau SDK Server API.)\n"
        "                       (default='siteID')\n"
        "\n" );
}

//------------------------------------------------------------------------------
//  Parse Arguments
//------------------------------------------------------------------------------
#define NUM_OPTIONS 12
enum OPTIONS {
    HELP = 0,
    BUILD,
    SPATIAL,
    PUBLISH,
    OVERWRITE,
    FILENAME,
    PROJECT_NAME,
    DATASOURCE_NAME,
    HOSTNAME,
    USERNAME,
    PASSWORD,
    SITE_ID
};
static TableauWChar *FALSE = (TableauWChar *)0x0;
static TableauWChar *TRUE = (TableauWChar *)0x1;

//  Parse Command Line Arguments or Populate Defaults
//
//  Returns 'true' if all arguments are successfully parsed
//  Returns 'false' if there are any invalid arguments or no arguments
//  (NOTE: if 'false' is returned, 'options' may contain NULL pointers)
//  (NOTE: if ParseArguments() is called, FreeArguments() must be called on the
//   same 'options' array before the program exits)
bool ParseArguments(
    int argc,
    char *argv[],
    TableauWChar *options[ NUM_OPTIONS ]
)
{
    if ( argc == 0 ){
        return false;
    }

    for ( int i = 0; i < argc; ++i ) {
        if ( !strcmp( argv[ i ], "-h" ) || !strcmp( argv[ i ], "--help" ) ) {
            options[ HELP ] = TRUE;
            return true;
        }
        else if ( !strcmp( argv[ i ], "-b" ) || !strcmp( argv[ i ], "--build" ) ) {
            options[ BUILD ] = TRUE;
        }
        else if ( !strcmp( argv[ i ], "-s" ) || !strcmp( argv[ i ], "--spatial" ) ) {
            options[ SPATIAL ] = TRUE;
        }
        else if ( !strcmp( argv[ i ], "-p" ) || !strcmp( argv[ i ], "--publish" ) ) {
            options[ PUBLISH ] = TRUE;
        }
        else if ( !strcmp( argv[ i ], "-o" ) || !strcmp( argv[ i ], "--overwrite" ) ) {
            options[ OVERWRITE ] = TRUE;
        }
        else if ( !strcmp( argv[ i ], "-f" ) || !strcmp( argv[ i ], "--filename" ) ) {
            i++;
            if (i >= argc) {
                return false;
            }
            CreateTableauStringOption( argv[ i ], options[ FILENAME ] );
        }
        else if ( !strcmp( argv[ i ], "--project-name" ) ) {
            i++;
            if (i >= argc) {
                return false;
            }
            CreateTableauStringOption( argv[ i ], options[ PROJECT_NAME ] );
        }
        else if ( !strcmp( argv[ i ], "--datasource-name" ) ) {
            i++;
            if (i >= argc) {
                return false;
            }
            CreateTableauStringOption( argv[ i ], options[ DATASOURCE_NAME ] );
        }
        else if ( !strcmp( argv[ i ], "--hostname" ) ) {
            i++;
            if (i >= argc) {
                return false;
            }
            CreateTableauStringOption( argv[ i ], options[ HOSTNAME ] );
        }
        else if ( !strcmp( argv[ i ], "--username" ) ) {
            i++;
            if (i >= argc) {
                return false;
            }
            CreateTableauStringOption( argv[ i ], options[ USERNAME ] );
        }
        else if ( !strcmp( argv[ i ], "--password" ) ) {
            i++;
            if (i >= argc) {
                return false;
            }
            CreateTableauStringOption( argv[ i ], options[ PASSWORD ] );
        }
        else if ( !strcmp( argv[ i ], "--site-id" ) ) {
            i++;
            if (i >= argc) {
                return false;
            }
            CreateTableauStringOption( argv[ i ], options[ SITE_ID ] );
        }
        else {
            return false;
        }
    }

    //  Defaults
    //  options[ BUILD ] = FALSE;
    //  options[ SPATIAL ] = FALSE;
    //  options[ PUBLISH ] = FALSE;
    //  options[ OVERWRITE ] == FALSE;
    if ( options[ FILENAME ] == NULL ) {
        CreateTableauStringOption( "order-c.tde", options[ FILENAME ] );
    }
    if ( options[ PROJECT_NAME ] == NULL ) {
        CreateTableauStringOption( "default", options[ PROJECT_NAME ] );
    }
    if ( options[ DATASOURCE_NAME ] == NULL ) {
        CreateTableauStringOption( "order-c", options[ DATASOURCE_NAME ] );
    }
    if ( options[ HOSTNAME ] == NULL ) {
        CreateTableauStringOption( "http://localhost", options[ HOSTNAME ] );
    }
    if ( options[ USERNAME ] == NULL ) {
        CreateTableauStringOption( "username", options[ USERNAME ] );
    }
    if ( options[ PASSWORD ] == NULL ) {
        CreateTableauStringOption( "password", options[ PASSWORD ] );
    }
    if ( options[ SITE_ID ] == NULL ) {
        CreateTableauStringOption( "siteID", options[ SITE_ID ] );
    }

    return true;
}

//  Remove Previously Parsed Command Line Arguments from the Stack
void FreeOptions(
    TableauWChar **options,
    size_t numOptions
)
{
    for ( int i = 0; i < numOptions; ++i ) {
        if ( options[ i ] != FALSE && options[ i ] != TRUE ) {
            free( options[ i ] );
        }
    }
}

//------------------------------------------------------------------------------
//  Create or Open Extract
//------------------------------------------------------------------------------
//  (NOTE: This function assumes that the Tableau SDK Extract API is initialized)
void
CreateOrOpenExtract(
    TAB_HANDLE         *extractHandlePtr,
    const TableauString filename,
    const bool          useSpatial
)
{
    TAB_HANDLE hSchema = NULL;
    TAB_HANDLE hTable = NULL;

    CreateTableauStringConst( L"Purchased", sPurchased );
    CreateTableauStringConst( L"Product", sProduct );
    CreateTableauStringConst( L"uProduct", sUProduct );
    CreateTableauStringConst( L"Price", sPrice );
    CreateTableauStringConst( L"Quantity", sQuantity );
    CreateTableauStringConst( L"Taxed", sTaxed );
    CreateTableauStringConst( L"Expiration Date", sExpirationDate );
    CreateTableauStringConst( L"Produkt", sProdukt );
    CreateTableauStringConst( L"Spatial", sSpatial );
    CreateTableauStringConst( L"Extract", sExtract );

    //  Create Extract Object
    //  (NOTE: TabExtractCreate() opens an existing extract with the given
    //   filename if one exists or creates a new extract with the given filename
    //   if one does not)
    ExtractTryOp( TabExtractCreate( extractHandlePtr, filename ) );

    //  Define Table Schema (If we are creating a new extract)
    //  (NOTE: in Tableau Data Engine, all tables must be named "Extract")
    int hasTable = 0;
    ExtractTryOp( TabExtractHasTable( *extractHandlePtr, sExtract, &hasTable ) );
    if ( !hasTable ) {
        ExtractTryOp( TabTableDefinitionCreate( &hSchema ) );
        ExtractTryOp( TabTableDefinitionSetDefaultCollation( hSchema, TAB_COLLATION_en_GB) );
        ExtractTryOp( TabTableDefinitionAddColumn(  hSchema, sPurchased, TAB_TYPE_DateTime ) );
        ExtractTryOp( TabTableDefinitionAddColumn( hSchema, sProduct, TAB_TYPE_CharString ) );
        ExtractTryOp( TabTableDefinitionAddColumn( hSchema, sUProduct, TAB_TYPE_UnicodeString ) );
        ExtractTryOp( TabTableDefinitionAddColumn( hSchema, sPrice, TAB_TYPE_Double ) );
        ExtractTryOp( TabTableDefinitionAddColumn( hSchema, sQuantity, TAB_TYPE_Integer ) );
        ExtractTryOp( TabTableDefinitionAddColumn( hSchema, sTaxed, TAB_TYPE_Boolean ) );
        ExtractTryOp( TabTableDefinitionAddColumn( hSchema, sExpirationDate, TAB_TYPE_Date ) );
        ExtractTryOp( TabTableDefinitionAddColumnWithCollation( hSchema, sProdukt, TAB_TYPE_CharString, TAB_COLLATION_de ) );
        if ( useSpatial ) {
            ExtractTryOp( TabTableDefinitionAddColumn( hSchema, sSpatial, TAB_TYPE_Spatial ) );
        }
        ExtractTryOp( TabExtractAddTable( *extractHandlePtr, sExtract, hSchema, &hTable ) );
        if ( hTable == NULL ) {
            fprintf(stderr, "A fatal error occurred while creating the table.\nExiting now.\n" );
            exit( EXIT_FAILURE );
        }
        ExtractTryOp( TabTableDefinitionClose( hSchema ) );
    }
}

//------------------------------------------------------------------------------
//  Populate Extract
//------------------------------------------------------------------------------
//  (NOTE: This function assumes that the Tableau SDK Extract API is initialized)
void
PopulateExtract(
    TAB_HANDLE extractHandle,
    bool useSpatial
)
{
    TAB_HANDLE hTable = NULL;
    TAB_HANDLE hRow = NULL;
    TAB_HANDLE hSchema = NULL;
    int i;

    CreateTableauStringConst( L"Extract", sExtract );
    CreateTableauStringConst( L"uniBeans", sUniBeans );

    //  Get Schema
    ExtractTryOp( TabExtractOpenTable( extractHandle, sExtract, &hTable ) );
    ExtractTryOp( TabTableGetTableDefinition( hTable, &hSchema ) );

    //  Insert Data
    ExtractTryOp( TabRowCreate( &hRow, hSchema ) );
    ExtractTryOp( TabRowSetDateTime( hRow, 0, 2012, 7, 3, 11, 40, 12, 4550 ) ); //  Purchased
    ExtractTryOp( TabRowSetCharString( hRow, 1, "Beans" ) );                    //  Product
    ExtractTryOp( TabRowSetString( hRow, 2, sUniBeans ) );                      //  Unicode Product
    ExtractTryOp( TabRowSetDouble( hRow, 3, 1.08 ) );                           //  Price
    ExtractTryOp( TabRowSetDate( hRow, 6, 2029, 1, 1 ) );                       //  Expiration Date
    ExtractTryOp( TabRowSetCharString( hRow, 7, "Bohnen" ) );                   //  Produkt
    for ( i = 0; i < 10; ++i ) {
        ExtractTryOp( TabRowSetInteger( hRow, 4, i * 10 ) );                    //  Quantity
        ExtractTryOp( TabRowSetBoolean( hRow, 5, i % 2 ) );                     //  Taxed
        ExtractTryOp( TabTableInsert( hTable, hRow ) );
    }
    if ( useSpatial ) {
        ExtractTryOp( TabRowSetSpatial( hRow, 8, "POINT (30 10)" ) );           //  Destination
    }

    //  Close Schema
    ExtractTryOp( TabRowClose( hRow ) );
    ExtractTryOp( TabTableDefinitionClose( hSchema ) );
}

//------------------------------------------------------------------------------
//  Publish Extract
//------------------------------------------------------------------------------
//  (NOTE: This function assumes that the Tableau SDK Server API is initialized)
void
PublishExtract(
    TableauString filename,
    TableauString projectName,
    TableauString datasourceName,
    bool          overwrite,
    TableauString hostname,
    TableauString username,
    TableauString password,
    TableauString siteID
)
{
    TAB_HANDLE serverHandle = NULL;

    // Create the Server Connection Object
    ServerTryOp( TabServerConnectionCreate( &serverHandle ) );

    // Connect to the Server
    ServerTryOp( TabServerConnectionConnect( serverHandle, hostname, username, password, siteID ) );

    // Publish the Extract to the Server
    ServerTryOp( TabServerConnectionPublishExtract( serverHandle, filename, projectName, datasourceName, overwrite ) );

    // Disconnect from the Server
    ServerTryOp( TabServerConnectionDisconnect( serverHandle ) );

    // Destroy the Server Connection Object
    ServerTryOp( TabServerConnectionClose( serverHandle ) );
}

//------------------------------------------------------------------------------
//  Main
//------------------------------------------------------------------------------
int
main( int argc, char* argv[] )
{
    //  Parse Arguments
    TableauWChar *options[ NUM_OPTIONS];
    memset( options, 0, NUM_OPTIONS * sizeof( TableauWChar* ) );
    if ( !ParseArguments( argc - 1, argv  + 1, options ) || options[ HELP ] != NULL ) {
        DisplayUsage();
        FreeOptions( options, NUM_OPTIONS );
        return 0;
    }

    //  Extract API Demo
    if ( options[ BUILD ] != FALSE ) {
        //  Initialize the Tableau Extract API
        TryOp( TabExtractAPIInitialize(), "initializing the Extract API", TabGetLastErrorMessage() );

        //  Create or Expand the Extract
        TAB_HANDLE extractHandle = NULL;
        CreateOrOpenExtract( &extractHandle, options[ FILENAME ], options[ SPATIAL ] != FALSE );
        PopulateExtract( extractHandle, options[ SPATIAL ] != FALSE );

        //  Flush the Extract to Disk
        TryOp( TabExtractClose( extractHandle ), "closing an extract", TabGetLastErrorMessage() );

        // Close the Tableau Extract API
        TryOp( TabExtractAPICleanup(), "cleaning up the Extract API", TabGetLastErrorMessage() );
    }

    //  Server API Demo
    if ( options[ PUBLISH ] != FALSE ) {
        //  Initialize the Tableau Server API
        TryOp( TabServerAPIInitialize(), "initializing the Server API", TabGetLastErrorMessage() );

        //  Publish the Extract
        PublishExtract(
            options[ FILENAME ], options[ PROJECT_NAME ], options[ DATASOURCE_NAME ], options[ OVERWRITE ] != FALSE,
            options[ HOSTNAME ], options[ USERNAME ], options[ PASSWORD ], options[ SITE_ID ] );

        //  Close the Tableau Server API
        TryOp( TabServerAPICleanup(), "cleaning up the Server API", TabGetLastErrorMessage() );
    }

    FreeOptions( options, NUM_OPTIONS );
    return 0;
}
