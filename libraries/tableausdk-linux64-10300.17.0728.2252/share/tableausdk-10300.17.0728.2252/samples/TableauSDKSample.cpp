//------------------------------------------------------------------------------
//
//  This file is the copyrighted property of Tableau Software and is protected
//  by registered patents and other applicable U.S. and international laws and
//  regulations.
//
//  Unlicensed use of the contents of this file is prohibited. Please refer to
//  the NOTICES.txt file for further details.
//
//------------------------------------------------------------------------------
#if defined(__APPLE__) && defined(__MACH__)
#include <TableauExtract/TableauExtract_cpp.h>
#include <TableauServer/TableauServer_cpp.h>
#else
#include "TableauExtract_cpp.h"
#include "TableauServer_cpp.h"
#endif

#include <codecvt>
#include <iostream>
#include <locale>
#include <map>
#include <string>

using namespace Tableau;

//------------------------------------------------------------------------------
//  Display Usage
//------------------------------------------------------------------------------
void DisplayUsage()
{
    std::cerr << "usage: tableauSDKSample    [-h] [-b] [-s] [-p] [-o] [-f FILENAME]" << std::endl
              << "                           [--project-name PROJECT_NAME]" << std::endl
              << "                           [--datasource-name DATASOURCE_NAME]" << std::endl
              << "                           [--hostname HOSTNAME] [--username USERNAME]" << std::endl
              << "                           [--password PASSWORD] [--site-id SITEID]" << std::endl
              << std::endl
              << "A simple demonstration of the Tableau SDK." << std::endl
              << std::endl
              << "optional arguments:" << std::endl
              << " -h, --help            show this help message and exit" << std::endl
              << " -b, --build           If an extract named FILENAME exists, open it and add data to"
              << "                       it. If no such extract exists, create one and add data to it." << std::endl
              << "                       If no FILENAME is specified, the default is used." << std::endl
              << "                       (default=False)" << std::endl
              << std::endl
              << " -s, --spatial         If creating a new extract, include spatial data when adding" << std::endl
              << "                       data. If '--build' is not specified, or the extract being" << std::endl
              << "                       built is not newly created, this argument is ignored." << std::endl
              << "                       (default=False)" << std::endl
              << std::endl
              << " -p, --publish         Publish an extract named FILENAME to a Tableau Server instance" << std::endl
              << "                       running at HOSTNAME, creating a published datasource named" << std::endl
              << "                       DATASOURCE_NAME on the server under the PROJECT_NAME project." << std::endl
              << std::endl
              << "                       If '--overwrite' is specified, if there is an existing" << std::endl
              << "                       published datasource on the server named DATASOURCE_NAME under" << std::endl
              << "                       the PROJECT_NAME project, it is overwritten." << std::endl
              << "                       USERNAME, PASSWORD, and SITEID are used to connect to the" << std::endl
              << "                       server." << std::endl
              << std::endl
              << "                       If any of '--filename', '--project-name', '--datasource-name'," << std::endl
              << "                       '--overwrite', '--hostname', '--username', '--password'," << std::endl
              << "                       or '--site-id' are not specified, the corresponding default" << std::endl
              << "                       value(s) are used." << std::endl
              << std::endl
              << "                       (NOTE: If '--username', '--password', and '--site-id' are not" << std::endl
              << "                        each specified, '--publish' will not succeed.)" << std::endl
              << "                       (default=False)" << std::endl
              << std::endl
              << " -o, --overwrite       Overwrite any existing published datasource named" << std::endl
              << "                       DATASOURCE_NAME under the PROJECT_NAME project on the server." << std::endl
              << "                       If '--project-name' and/or '--datasource-name' are not" << std::endl
              << "                       specified, the corresponding default value(s) are used. If" << std::endl
              << "                       '--publish' is not specified, this argument is ignored" << std::endl
              << "                       (default=False)" << std::endl
              << std::endl
              << " -f FILENAME, --filename FILENAME" << std::endl
              << "                       Use FILENAME as the extract filename when creating, opening," << std::endl
              << "                       and/or publishing an extract. If neither '--build' nor" << std::endl
              << "                       '--publish' is specified, this argument is ignored." << std::endl
              << "                       (default='order-cpp.tde')" << std::endl
              << std::endl
              << " --project-name PROJECT_NAME" << std::endl
              << "                       Use PROJECT_NAME as the project-name when creating publishing" << std::endl
              << "                       an extract. If '--publish' is not specified, this argument is" << std::endl
              << "                       ignored." << std::endl
              << "                       (default='default')" << std::endl
              << std::endl
              << " --datasource-name DATASOURCE_NAME" << std::endl
              << "                       Use DATASOURCE_NAME as the datasource name when creating" << std::endl
              << "                       publishing an extract. If '--publish' is not specified, this" << std::endl
              << "                       argument is ignored." << std::endl
              << "                       (default='order-cpp')" << std::endl
              << std::endl
              << " --hostname HOSTNAME   Connect to a Tableau Server instance running at HOSTNAME to" << std::endl
              << "                       publish an extract. If '--publish' is not specified, this" << std::endl
              << "                       argument is ignored." << std::endl
              << "                       (default='http://localhost')" << std::endl
              << std::endl
              << " --username USERNAME   Connect to the server as user USERNAME to publish an extract." << std::endl
              << "                       If '--publish' is not specified, this argument is ignored." << std::endl
              << std::endl
              << "                       (NOTE: This argument must be specified for '--publish' to" << std::endl
              << "                        succeed. Admin privileges are required in Tableau Server to" << std::endl
              << "                        publish datasources using the Tableau SDK Server API.)" << std::endl
              << "                       (default='username')" << std::endl
              << std::endl
              << " --password PASSWORD   Connect to the server using password PASSWORD to publish an" << std::endl
              << "                       extract. If '--publish' is not specified, this argument is" << std::endl
              << "                       ignored." << std::endl
              << std::endl
              << "                       (NOTE: This argument must be specified for '--publish' to" << std::endl
              << "                        succeed. Admin privileges are required in Tableau Server to" << std::endl
              << "                        publish datasources using the Tableau SDK Server API.)" << std::endl
              << "                       (default='password')" << std::endl
              << std::endl
              << " --site-id SITEID      Connect to the server using siteID SITEID to publish an" << std::endl
              << "                       extract. If '--publish' is not specified, this argument is" << std::endl
              << "                       ignored." << std::endl
              << std::endl
              << "                       (NOTE: This argument must be specified for '--publish' to" << std::endl
              << "                        succeed. Admin privileges are required in Tableau Server to" << std::endl
              << "                        publish datasources using the Tableau SDK Server API.)" << std::endl
              << "                       (default='siteID')" << std::endl
              << std::endl;
}

//------------------------------------------------------------------------------
//  Parse Arguments
//------------------------------------------------------------------------------
//  Parse Command Line Arguments or Populate Defaults
//
//  Returns 'true' if all arguments are successfully parsed
//  Returns 'false' if there are any invalid arguments or no arguments
bool ParseArguments(int argc, char* argv[], std::map<std::string, std::wstring>& options)
{
    if (argc == 0)
    {
        return false;
    }

    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    for (int i = 0; i < argc; ++i)
    {
        if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help"))
        {
            options[std::string("help")] = std::wstring(L"true");
            return true;
        }
        else if (!strcmp(argv[i], "-b") || !strcmp(argv[i], "--build"))
        {
            options[std::string("build")] = std::wstring(L"true");
        }
        else if (!strcmp(argv[i], "-s") || !strcmp(argv[i], "--spatial"))
        {
            options[std::string("spatial")] = std::wstring(L"true");
        }
        else if (!strcmp(argv[i], "-p") || !strcmp(argv[i], "--publish"))
        {
            options[std::string("publish")] = std::wstring(L"true");
        }
        else if (!strcmp(argv[i], "-o") || !strcmp(argv[i], "--overwrite"))
        {
            options[std::string("overwrite")] = std::wstring(L"true");
        }
        else if (!strcmp(argv[i], "-f") || !strcmp(argv[i], "--filename"))
        {
            if (i >= argc)
            {
                return false;
            }
            options[std::string("filename")] = converter.from_bytes(argv[++i]);
        }
        else if (!strcmp(argv[i], "--project-name"))
        {
            if (i + 1 >= argc)
            {
                return false;
            }
            options[std::string("project-name")] = converter.from_bytes(argv[++i]);
        }
        else if (!strcmp(argv[i], "--datasource-name"))
        {
            if (i + 1 >= argc)
            {
                return false;
            }
            options[std::string("datasource-name")] = converter.from_bytes(argv[++i]);
        }
        else if (!strcmp(argv[i], "--hostname"))
        {
            if (i + 1 >= argc)
            {
                return false;
            }
            options[std::string("hostname")] = converter.from_bytes(argv[++i]);
        }
        else if (!strcmp(argv[i], "--username"))
        {
            if (i + 1 >= argc)
            {
                return false;
            }
            options[std::string("username")] = converter.from_bytes(argv[++i]);
        }
        else if (!strcmp(argv[i], "--password"))
        {
            if (i + 1 >= argc)
            {
                return false;
            }
            options[std::string("password")] = converter.from_bytes(argv[++i]);
        }
        else if (!strcmp(argv[i], "--site-id"))
        {
            if (i + 1 >= argc)
            {
                return false;
            }
            options[std::string("site-id")] = converter.from_bytes(argv[++i]);
        }
        else
        {
            return false;
        }
    }

    //  Defaults
    //  options[ "build" ] = "false";
    //  options[ "spatial" ] = "false";
    //  options[ "publish" ] = "false";
    //  options[ "overwrite" ] = "false";
    if (!options.count("filename"))
    {
        options["filename"] = L"order-cpp.tde";
    }
    if (!options.count("project-name"))
    {
        options["project-name"] = L"default";
    }
    if (!options.count("datasource-name"))
    {
        options["datasource-name"] = L"order-cpp";
    }
    if (!options.count("hostname"))
    {
        options["hostname"] = L"hostname";
    }
    if (!options.count("username"))
    {
        options["username"] = L"username";
    }
    if (!options.count("password"))
    {
        options["password"] = L"password";
    }
    if (!options.count("site-id"))
    {
        options["site-id"] = L"siteID";
    }

    return true;
}

//------------------------------------------------------------------------------
//  Create or Open Extract
//------------------------------------------------------------------------------
std::shared_ptr<Extract> CreateExtract(const std::wstring& fileName, const bool useSpatial)
{
    std::shared_ptr<Extract> extractPtr = nullptr;
    std::shared_ptr<Table> tablePtr = nullptr;
    try
    {
        //  Create Extract Object
        //  (NOTE: TabExtractCreate() opens an existing extract with the given
        //   filename if one exists or creates a new extract with the given filename
        //   if one does not)
        extractPtr = std::make_shared<Extract>(fileName);

        //  Define Table Schema
        //  (NOTE: in Tableau Data Engine, all tables must be named "Extract")
        if (!extractPtr->HasTable(L"Extract"))
        {
            TableDefinition schema;
            schema.SetDefaultCollation(Collation_en_GB);
            schema.AddColumn(L"Purchased", Type_DateTime);
            schema.AddColumn(L"Product", Type_CharString);
            schema.AddColumn(L"uProduct", Type_UnicodeString);
            schema.AddColumn(L"Price", Type_Double);
            schema.AddColumn(L"Quantity", Type_Integer);
            schema.AddColumn(L"Taxed", Type_Boolean);
            schema.AddColumn(L"Expiration Date", Type_Date);
            schema.AddColumnWithCollation(L"Produkt", Type_CharString, Collation_de);
            if (useSpatial)
            {
                schema.AddColumn(L"Destination", Type_Spatial);
            }

            tablePtr = extractPtr->AddTable(L"Extract", schema);
            if (tablePtr == nullptr)
            {
                std::wcerr << L"A fatal error occurred while creating the new table" << std::endl
                           << L"Exiting Now." << std::endl;
                exit(EXIT_FAILURE);
            }
        }
    }
    catch (const TableauException& e)
    {
        std::wcerr << L"A fatal error occurred while creating the new extract: " << std::endl
                   << e.GetMessage() << std::endl
                   << L"Exiting Now." << std::endl;
        exit(EXIT_FAILURE);
    }

    return extractPtr;
}

//------------------------------------------------------------------------------
//  Populate Extract
//------------------------------------------------------------------------------
void PopulateExtract(const std::shared_ptr<Extract> extractPtr, bool useSpatial)
{
    try
    {
        //  Get Schema
        std::shared_ptr<Table> tablePtr = extractPtr->OpenTable(L"Extract");
        std::shared_ptr<Tableau::TableDefinition> schema = tablePtr->GetTableDefinition();

        //  Insert Data
        Tableau::Row row(*schema);
        row.SetDateTime(0, 2012, 7, 3, 11, 40, 12, 4550); //  Purchased
        row.SetCharString(1, "Beans");                    //  Product
        row.SetString(2, L"uniBeans");                    //  Unicode Product
        row.SetDouble(3, 1.08);                           //  Price
        row.SetDate(6, 2029, 1, 1);                       //  Expiration Date
        row.SetCharString(7, "Bohnen");                   //  Produkt
        for (int i = 0; i < 10; ++i)
        {
            row.SetInteger(4, i * 10);     //  Quantity
            row.SetBoolean(5, i % 2 == 1); //  Taxed
            tablePtr->Insert(row);
        }
        if (useSpatial)
        {
            row.SetSpatial(8, "POINT (30 10)"); //  Destination
        }
    }
    catch (const TableauException& e)
    {
        std::wcerr << L"A fatal error occurred while populating the extract: " << std::endl
                   << e.GetMessage() << std::endl
                   << L"Exiting Now." << std::endl;
        exit(EXIT_FAILURE);
    }
}

//------------------------------------------------------------------------------
//  Publish Extract
//------------------------------------------------------------------------------
void PublishExtract(
    const std::wstring& filename,
    const std::wstring& projectName,
    const std::wstring& datasourceName,
    const bool overwrite,
    const std::wstring& hostname,
    const std::wstring& username,
    const std::wstring& password,
    const std::wstring& siteID)
{
    try
    {
        // Create the ServerConnection Object
        ServerConnection serverConnection;

        // Connect to the Server
        serverConnection.Connect(hostname, username, password, siteID);

        // Publish the Extract to the Server
        serverConnection.PublishExtract(filename, projectName, datasourceName, overwrite);

        // Disconnect from the Server
        serverConnection.Disconnect();

        // Destroy the ServerConnection Object
        serverConnection.Close();
    }
    catch (const TableauException& e)
    {
        std::wcerr << L"A fatal error occurred while publishing the extract: " << std::endl;
        switch (e.GetResultCode())
        {
            case Result::Result_InternalError:
                std::wcerr << L"InternalError - Could not parse the response from the server." << std::endl;
                break;
            case Result::Result_InvalidArgument:
                std::wcerr << L"InvalidArgument - " << e.GetMessage() << std::endl;
                break;
            case Result::Result_CurlError:
                std::wcerr << L"CurlError - " << e.GetMessage() << std::endl;
                break;
            case Result::Result_ServerError:
                std::wcerr << L"ServerError - " << e.GetMessage() << std::endl;
                break;
            case Result::Result_NotAuthenticated:
                std::wcerr << L"NotAuthenticated - " << e.GetMessage() << std::endl;
                break;
            case Result::Result_BadPayload:
                std::wcerr
                    << L"BadPayload - Unknown response from the server. Make sure this version of Tableau API is compatible with your server."
                    << std::endl;
                break;
            case Result::Result_InitError:
                std::wcerr << L"InitError - " << e.GetMessage() << std::endl;
                break;
            case Result::Result_UnknownError:
            default:
                std::wcerr << L"An unknown error occurred." << std::endl;
        }
        std::wcerr << L"Exiting now." << std::endl;
        exit(EXIT_FAILURE);
    }
}

//------------------------------------------------------------------------------
//  Main
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
    //  Parse Arguments
    std::map<std::string, std::wstring> options;
    if (!ParseArguments(argc - 1, argv + 1, options) || options.count("help"))
    {
        DisplayUsage();
        exit(EXIT_SUCCESS);
    }

    //  Extract API Demo
    if (options.count("build") > 0)
    {
        //  Initialize the Tableau Extract API
        ExtractAPI::Initialize();

        //  Create or Expand the Extract
        std::shared_ptr<Extract> extractPtr = nullptr;
        extractPtr = std::move(CreateExtract(options["filename"], options.count("spatial") > 0));
        PopulateExtract(extractPtr, options.count("spatial") > 0);

        //  Flush the Extract to Disk
        extractPtr->Close();

        //  Close the Tableau Extract API
        ExtractAPI::Cleanup();
    }

    //  Server API Demo
    if (options.count("publish") > 0)
    {
        // Initialize the Tableau Server API
        ServerAPI::Initialize();

        //  Publish the Extract
        PublishExtract(
            options["filename"], options["project-name"], options["datasource-name"], options.count("overwrite") > 0,
            options["hostname"], options["username"], options["password"], options["site-id"]);

        //  Close the Tableau Server API
        ServerAPI::Cleanup();
    }

    exit(EXIT_SUCCESS);
}
