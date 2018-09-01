/**
 * -----------------------------------------------------------------------------
 *
 *  This file is the copyrighted property of Tableau Software and is protected
 *  by registered patents and other applicable U.S. and international laws and
 *  regulations.
 *
 *  Unlicensed use of the contents of this file is prohibited. Please refer to
 *  the NOTICES.txt file for further details.
 *
 * -----------------------------------------------------------------------------
 */
package com.tableausoftware.demos;

import java.util.HashMap;

import com.tableausoftware.TableauException;
import com.tableausoftware.common.*;
import com.tableausoftware.extract.*;
import com.tableausoftware.server.*;

public final class TableauSDKSample {
    /**
     * -------------------------------------------------------------------------
     *  Display Usage
     * -------------------------------------------------------------------------
     */
    private static void displayUsage() {
        System.err.print(""
            + "usage: tableauSDKSample    [-h] [-b] [-s] [-p] [-o] [-f FILENAME]\n"
            + "                           [--project-name PROJECT_NAME]\n"
            + "                           [--datasource-name DATASOURCE_NAME]\n"
            + "                           [--hostname HOSTNAME] [--username USERNAME]\n"
            + "                           [--password PASSWORD] [--site-id SITEID]\n"
            + "\n"
            + "A simple demonstration of the Tableau SDK.\n"
            + "\n"
            + "optional arguments:\n"
            + " -h, --help            show this help message and exit\n"
            + " -b, --build           If an extract named FILENAME exists, open it and add data to\n"
            + "                       it. If no such extract exists, create one and add data to it.\n"
            + "                       If no FILENAME is specified, the default is used.\n"
            + "                       (default=False)\n"
            + "\n"
            + " -s, --spatial         If creating a new extract, include spatial data when adding\n"
            + "                       data. If '--build' is not specified, or the extract being\n"
            + "                       built is not newly created, this argument is ignored.\n"
            + "                       (default=False)\n"
            + "\n"
            + " -p, --publish         Publish an extract named FILENAME to a Tableau Server instance\n"
            + "                       running at HOSTNAME, creating a published datasource named\n"
            + "                       DATASOURCE_NAME on the server under the PROJECT_NAME project.\n"
            + "\n"
            + "                       If '--overwrite' is specified, if there is an existing\n"
            + "                       published datasource on the server named DATASOURCE_NAME under\n"
            + "                       the PROJECT_NAME project, it is overwritten.\n"
            + "                       USERNAME, PASSWORD, and SITEID are used to connect to the\n"
            + "                       server.\n"
            + "\n"
            + "                       If any of '--filename', '--project-name', '--datasource-name',\n"
            + "                       '--overwrite', '--hostname', '--username', '--password',\n"
            + "                       or '--site-id' are not specified, the corresponding default\n"
            + "                       value(s) are used.\n"
            + "\n"
            + "                       (NOTE: If '--username', '--password', and '--site-id' are not\n"
            + "                        each specified, '--publish' will not succeed.)\n"
            + "                       (default=False)\n"
            + "\n"
            + " -o, --overwrite       Overwrite any existing published datasource named\n"
            + "                       DATASOURCE_NAME under the PROJECT_NAME project on the server.\n"
            + "                       If '--project-name' and/or '--datasource-name' are not\n"
            + "                       specified, the corresponding default value(s) are used. If\n"
            + "                       '--publish' is not specified, this argument is ignored\n"
            + "                       (default=False)\n"
            + "\n"
            + " -f FILENAME, --filename FILENAME\n"
            + "                       Use FILENAME as the extract filename when creating, opening,\n"
            + "                       and/or publishing an extract. If neither '--build' nor\n"
            + "                       '--publish' is specified, this argument is ignored.\n"
            + "                       (default='order-java.tde')\n"
            + "\n"
            + " --project-name PROJECT_NAME\n"
            + "                       Use PROJECT_NAME as the project-name when creating publishing\n"
            + "                       an extract. If '--publish' is not specified, this argument is\n"
            + "                       ignored.\n"
            + "                       (default='default')\n"
            + "\n"
            + " --datasource-name DATASOURCE_NAME\n"
            + "                       Use DATASOURCE_NAME as the datasource name when creating\n"
            + "                       publishing an extract. If '--publish' is not specified, this\n"
            + "                       argument is ignored.\n"
            + "                       (default='order-java')\n"
            + "\n"
            + " --hostname HOSTNAME   Connect to a Tableau Server instance running at HOSTNAME to\n"
            + "                       publish an extract. If '--publish' is not specified, this\n"
            + "                       argument is ignored.\n"
            + "                       (default='http://localhost')\n"
            + "\n"
            + " --username USERNAME   Connect to the server as user USERNAME to publish an extract.\n"
            + "                       If '--publish' is not specified, this argument is ignored.\n"
            + "\n"
            + "                       (NOTE: This argument must be specified for '--publish' to\n"
            + "                        succeed. Admin privileges are required in Tableau Server to\n"
            + "                        publish datasources using the Tableau SDK Server API.)\n"
            + "                       (default='username')\n"
            + "\n"
            + " --password PASSWORD   Connect to the server using password PASSWORD to publish an\n"
            + "                       extract. If '--publish' is not specified, this argument is\n"
            + "                       ignored.\n"
            + "\n"
            + "                       (NOTE: This argument must be specified for '--publish' to\n"
            + "                        succeed. Admin privileges are required in Tableau Server to\n"
            + "                        publish datasources using the Tableau SDK Server API.)\n"
            + "                       (default='password')\n"
            + "\n"
            + " --site-id SITEID      Connect to the server using siteID SITEID to publish an\n"
            + "                       extract. If '--publish' is not specified, this argument is\n"
            + "                       ignored.\n"
            + "\n"
            + "                       (NOTE: This argument must be specified for '--publish' to\n"
            + "                        succeed. Admin privileges are required in Tableau Server to\n"
            + "                        publish datasources using the Tableau SDK Server API.)\n"
            + "                       (default='siteID')\n"
            + "\n" );
    }


    /**
     * -------------------------------------------------------------------------
     *  Parse Arguments
     * -------------------------------------------------------------------------
     *  (NOTE: This function returns 'null' if there are any invalid arguments)
     */
    private static HashMap<String, String> parseArguments(
        String[] arguments
    )
    {
        if ( arguments.length == 0 ) {
            return null;
        }

        HashMap<String, String> options = new HashMap<String, String>();
        for ( int i = 0; i < arguments.length; ++i ) {
            switch( arguments[ i ] ) {
            case "-h":
            case "--help":
                options.put( "help", "true" );
                return options;
            case "-b":
            case "--build":
                options.put( "build", "true" );
                break;
            case "-s":
            case "--spatial":
                options.put( "spatial", "true" );
                break;
            case "-p":
            case "--publish":
                options.put( "publish", "true" );
                break;
            case "-o":
            case "--overwrite":
                options.put( "overwrite", "true" );
                break;
            case "-f":
            case "--filename":
                if ( i + 1 >= arguments.length ) {
                    return null;
                }
                options.put( "filename", arguments[ ++i ] );
                break;
            case "--project-name":
                if ( i + 1 >= arguments.length ) {
                    return null;
                }
                options.put( "project-name", arguments[ ++i ] );
                break;
            case "--datasource-name":
                if ( i + 1 >= arguments.length ) {
                    return null;
                }
                options.put( "datasource-name", arguments[ ++i ] );
                break;
            case "--hostname":
                if ( i + 1 >= arguments.length ) {
                    return null;
                }
                options.put( "hostname", arguments[ ++i ] );
                break;
            case "--username":
                if ( i + 1 >= arguments.length ) {
                    return null;
                }
                options.put( "username", arguments[ ++i ] );
                break;
            case "--password":
                if ( i + 1 >= arguments.length ) {
                    return null;
                }
                options.put( "password", arguments[ ++i ] );
                break;
            case "--site-id":
                if ( i + 1 >= arguments.length ) {
                    return null;
                }
                options.put( "site-id", arguments[ ++i ] );
                break;
            default:
                return null;
            }
        }

        //  Defaults
        //  options.put( "build" ) = false;
        //  options.put( "spatial" ) = false;
        //  options.put( "publish" ) = false;
        //  options.put( "overwrite" ) = false;
        if ( !options.containsKey( "filename" ) ) {
            options.put( "filename", "order-java.tde" );
        }
        if ( !options.containsKey( "project-name" ) ) {
            options.put( "project-name", "default" );
        }
        if ( !options.containsKey( "datasource-name" ) ) {
            options.put( "datasource-name", "order-java" );
        }
        if ( !options.containsKey( "hostname" ) ) {
            options.put( "hostname", "http://localhost" );
        }
        if ( !options.containsKey( "username" ) ) {
            options.put( "username", "username" );
        }
        if ( !options.containsKey( "password" ) ) {
            options.put( "password", "password" );
        }
        if ( !options.containsKey( "site-id" ) ) {
            options.put( "site-id", "siteID" );
        }

        return options;
    }

    /**
     * -------------------------------------------------------------------------
     *  Create or Open Extract
     * -------------------------------------------------------------------------
     *  (NOTE: This function assumes that the Tableau SDK Extract API is initialized)
     */
    private static Extract createOrOpenExtract(
        String filename,
        boolean useSpatial
    )
    {
        Extract extract = null;
        Table table = null;
        try {
            //  Create Extract Object
            //  (NOTE: TabExtractCreate() opens an existing extract with the given
            //   filename if one exists or creates a new extract with the given filename
            //   if one does not)
            extract = new Extract( filename );

            //  Define Table Schema (If we are creating a new extract)
            //  (NOTE: in Tableau Data Engine, all tables must be named "Extract")
            if ( !extract.hasTable( "Extract" ) ) {
                TableDefinition schema = new TableDefinition();
                schema.setDefaultCollation( Collation.EN_GB );
                schema.addColumn( "Purchased",       Type.DATETIME );
                schema.addColumn( "Product",         Type.CHAR_STRING );
                schema.addColumn( "uProduct",        Type.UNICODE_STRING );
                schema.addColumn( "Price",           Type.DOUBLE );
                schema.addColumn( "Quantity",        Type.INTEGER );
                schema.addColumn( "Taxed",           Type.BOOLEAN );
                schema.addColumn( "Expiration Date", Type.DATE );
                schema.addColumnWithCollation( "Produkt", Type.CHAR_STRING, Collation.DE );
                if ( useSpatial ) {
                    schema.addColumn( "Destination", Type.SPATIAL );
                }
                table = extract.addTable( "Extract", schema );
                if ( table == null ) {
                    System.err.println( "A fatal error occured while creating the table" );
                    System.err.println( "Exiting now." );
                    System.exit( -1 );
                }
            }
        }
        catch ( TableauException e ) {
            System.err.println( "A fatal error occurred while creating the extract:" );
            System.err.println( e.getMessage() );
            System.err.println( "Printing stack trace now:" );
            e.printStackTrace( System.err );
            System.err.println( "Exiting now." );
            System.exit( -1 );
        }
        catch ( Throwable t ) {
            System.err.println( "An unknown error occured while creating the extract" );
            System.err.println( "Printing stack trace now:" );
            t.printStackTrace( System.err );
            System.err.println( "Exiting now." );
            System.exit( -1 );
        }

        return extract;
    }

    /**
     * -------------------------------------------------------------------------
     *  Populate Extract
     * -------------------------------------------------------------------------
     *  (NOTE: This function assumes that the Tableau SDK Extract API is initialized)
     */
    private static void populateExtract(
        Extract extract,
        boolean useSpatial
    )
    {
        try {
            //  Get Schema
            Table table = extract.openTable( "Extract" );
            TableDefinition tableDef = table.getTableDefinition();

            //  Insert Data
            Row row = new Row( tableDef );
            row.setDateTime( 0, 2012, 7, 3, 11, 40, 12, 4550 ); //  Purchased
            row.setCharString( 1, "Beans" );                    //  Product
            row.setString( 2, "uniBeans" );                     //  Unicode Product
            row.setDouble( 3, 1.08 );                           //  Price
            row.setDate( 6, 2029, 1, 1 );                       //  Expiration Date
            row.setCharString( 7, "Bohnen" );                   //  Produkt
            for ( int i = 0; i < 10; ++i  ) {
                row.setInteger( 4, i * 10 );                    //  Quantity
                row.setBoolean( 5, i % 2 == 1 );                //  Taxed
                table.insert( row );
            }
            if ( useSpatial ) {
                row.setSpatial( 8, "POINT (30 10)" );           //  Destination
            }
        }
        catch ( TableauException e ) {
            System.err.println( "A fatal error occurred while populating the extract:" );
            System.err.println( e.getMessage() );
            System.err.println( "Printing stack trace now:" );
            e.printStackTrace( System.err );
            System.err.println( "Exiting now." );
            System.exit( -1 );
            }
        catch ( Throwable t ) {
            System.err.println( "An unknown error occured while populating the extract" );
            System.err.println( "Printing stack trace now:" );
            t.printStackTrace( System.err );
            System.err.println( "Exiting now." );
            System.exit( -1 );
        }
    }

    /**
     * -------------------------------------------------------------------------
     *  Publish Extract
     * -------------------------------------------------------------------------
     *  (NOTE: This function assumes that the Tableau SDK Server API is initialized)
     */
    private static void publishExtract(
        String  filename,
        String  projectName,
        String  datasourceName,
        boolean overwrite,
        String  hostname,
        String  username,
        String  password,
        String  siteID
    )
    {
        try {
            // Create the server connection object
            ServerConnection serverConnection = new ServerConnection();

            // Connect to the server
            serverConnection.connect( hostname, username, password, siteID );

            // Publish order-java.tde to the server under the default project with name Order-java
            serverConnection.publishExtract( filename, projectName, datasourceName, overwrite );

            // Disconnect from the server
            serverConnection.disconnect();

            // Destroy the server connection object
            serverConnection.close();
        }
        catch ( TableauException e ) {
            System.err.println( "A fatal error occurred while publishing the extract:" );
            switch( Result.enumForValue( e.getErrorCode() ) ) {
            case INTERNAL_ERROR:
                System.err.println( "INTERNAL_ERROR - Could not parse the response from the server." );
                break;
            case INVALID_ARGUMENT:
                System.err.println( "INVALID_ARGUMENT - " + e.getMessage() );
                break;
            case CURL_ERROR:
                System.err.println( "CURL_ERROR - " + e.getMessage() );
                break;
            case SERVER_ERROR:
                System.err.println( "SERVER_ERROR - " + e.getMessage() );
                break;
            case NOT_AUTHENTICATED:
                System.err.println( "NOT_AUTHENTICATED - " + e.getMessage() );
                break;
            case BAD_PAYLOAD:
                System.err.println( "BAD_PAYLOAD - Unknown response from the server. Make sure this version of Tableau API is compatible with your server." );
                break;
            case INIT_ERROR:
                System.err.println( "INIT_ERROR - " + e.getMessage() );
                break;
            case UNKNOWN_ERROR:
            default:
                System.err.println( "An unknown error occured." );
                break;
            }
            System.err.println( "Printing stack trace now:" );
            e.printStackTrace( System.err );
            System.err.println( "Exiting now." );
            System.exit( -1 );
        }
        catch ( Throwable t ) {
            System.err.println( "An unknown error occured while publishing the extract" );
            System.err.println( "Printing stack trace now:" );
            t.printStackTrace( System.err );
            System.err.println( "Exiting now." );
            System.exit( -1 );
        }
    }

    /**
     * -------------------------------------------------------------------------
     *  Main
     * -------------------------------------------------------------------------
     */
    public static void main( String[] arguments ) {
        //  Parse Arguments
        HashMap<String, String> options = parseArguments( arguments );
        if ( options == null || options.containsKey( "help" ) ) {
            displayUsage();
            return;
        }

        //  Extract API Demo
        if ( options.containsKey( "build" ) ) {
            try {
                //  Initialize the Tableau Extract API
                ExtractAPI.initialize();

                //  Create or Expand the Extract
                Extract extract = createOrOpenExtract( options.get( "filename" ), options.containsKey( "spatial" ) );
                populateExtract( extract, options.containsKey( "spatial" ) );

                //  Flush the Extract to Disk
                extract.close();

                // Close the Tableau Extract API
                ExtractAPI.cleanup();
            }
            catch ( TableauException e ) {
                System.err.println( "A fatal error occurred while opening or closing the Extract API:" );
                System.err.println( e.getMessage() );
                System.err.println( "Printing stack trace now:" );
                e.printStackTrace( System.err );
                System.err.println( "Exiting now." );
                System.exit( -1 );
            }
            catch ( Throwable t ) {
                System.err.println( "An unknown error occured while opening or closing the Extract API:" );
                System.err.println( "Printing stack trace now:" );
                t.printStackTrace( System.err );
                System.err.println( "Exiting now." );
                System.exit( -1 );
            }
        }

        //  Server API Demo
        if ( options.containsKey( "publish" ) ) {
            try {
                // Initialize Tableau Server API
                ServerAPI.initialize();

                //  Publish the Extract
                publishExtract( options.get( "filename" ), options.get( "project-name" ), options.get( "datasource-name" ), options.containsKey( "overwrite" ),
                    options.get( "hostname" ), options.get( "username" ), options.get( "password" ), options.get( "site-id" ) );

                //  Close the Tableau Server API
                ServerAPI.cleanup();
            }
            catch ( TableauException e ) {
                System.err.println( "A fatal error occurred while opening or closing the Server API:" );
                System.err.println( e.getMessage() );
                System.err.println( "Printing stack trace now:" );
                e.printStackTrace( System.err );
                System.err.println( "Exiting now." );
                System.exit( -1 );
            }
            catch ( Throwable t ) {
                System.err.println( "An unknown error occured while opening or closing the Server API:" );
                System.err.println( "Printing stack trace now:" );
                t.printStackTrace( System.err );
                System.err.println( "Exiting now." );
                System.exit( -1 );
            }
        }
    }
}
