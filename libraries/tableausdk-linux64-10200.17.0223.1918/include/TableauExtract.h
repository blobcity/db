// -----------------------------------------------------------------------
// Copyright (c) 2012 Tableau Software, Incorporated
//                    and its licensors. All rights reserved.
// Protected by U.S. Patent 7,089,266; Patents Pending.
//
// Portions of the code
// Copyright (c) 2002 The Board of Trustees of the Leland Stanford
//                    Junior University. All rights reserved.
// -----------------------------------------------------------------------
// TableauExtract.h
// -----------------------------------------------------------------------
// WARNING: Computer generated file.  Do not hand modify.

#ifndef TableauExtract_H
#define TableauExtract_H

#include "TableauCommon.h"

#if defined(_WIN32)
#  ifdef TBL_TABLEAUEXTRACT_BUILD
#    define TAB_API_EXTRACT __declspec(dllexport)
#  else
#    define TAB_API_EXTRACT __declspec(dllimport)
#  endif
#else
#    define TAB_API_EXTRACT __attribute__ ((visibility ("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*------------------------------------------------------------------------
  SECTION
  TableDefinition

  Represents the schema for a table in a Tableau data extract. The schema consists of a collection of column definitions, or more specifically name/type pairs.

  ------------------------------------------------------------------------*/

/// Initializes a new instance of the TableDefinition class.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionCreate(
    TAB_HANDLE *handle
);

/// Closes the TableDefinition object and frees associated memory.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionClose(TAB_HANDLE handle);

/// Returns the default collation for the table definition. The default is Collation.BINARY (0). You can change the default collation by calling <b>SetDefaultCollaction</b>.
/// @param retval The default collation.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionGetDefaultCollation(
    TAB_HANDLE TableDefinition
    , TAB_COLLATION* retval
);

/// Sets the default collation for new string columns.
/// @param collation The default collation for new string columns.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionSetDefaultCollation(
    TAB_HANDLE TableDefinition
    , TAB_COLLATION collation
);

/// Adds a column to the table definition. The order in which columns are added specifies their column number. String columns are defined with the current default collation.
/// @param name The name of the column to add.
/// @param type The data type of the column to add.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionAddColumn(
    TAB_HANDLE TableDefinition
    , TableauString name
    , TAB_TYPE type
);

/// Adds a column that has the specified collation.
/// @param name The name of the column to add.
/// @param type The data type of the column to add.
/// @param collation For string columns, the collation to use. For other types of columns, this value is ignored.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionAddColumnWithCollation(
    TAB_HANDLE TableDefinition
    , TableauString name
    , TAB_TYPE type
    , TAB_COLLATION collation
);

/// Returns the number of columns in the table definition.
/// @param retval The number of columns.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionGetColumnCount(
    TAB_HANDLE TableDefinition
    , int* retval
);

/// Returns the name of the specified column.
/// @param columnNumber The column number (zero-based) to return the name for.
/// @param retval The column name.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionGetColumnName(
    TAB_HANDLE TableDefinition
    , int columnNumber
    , TableauString* retval
);

/// Returns the data type of the specified column.
/// @param columnNumber The column number (zero-based) to return the name for.
/// @param retval The column data type.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionGetColumnType(
    TAB_HANDLE TableDefinition
    , int columnNumber
    , TAB_TYPE* retval
);

/// Returns the collation of the specified column.
/// @param columnNumber The column number (zero-based) to return the collation for.
/// @param retval The column collation.
TAB_API_EXTRACT TAB_RESULT TabTableDefinitionGetColumnCollation(
    TAB_HANDLE TableDefinition
    , int columnNumber
    , TAB_COLLATION* retval
);


/*------------------------------------------------------------------------
  SECTION
  Row

  Defines a row to be inserted into a table in an extract. The row is structured as a tuple.

  ------------------------------------------------------------------------*/

/// Initializes a new instance of the Row class. This method creates an empty row that has the specified schema.
/// @param tableDefinition The schema to use.
TAB_API_EXTRACT TAB_RESULT TabRowCreate(
    TAB_HANDLE *handle
    , TAB_HANDLE tableDefinition
);

/// Closes the row and frees associated resources.
TAB_API_EXTRACT TAB_RESULT TabRowClose(TAB_HANDLE handle);

/// Sets the specified column in the row to null.
/// @param columnNumber The column number (zero-based) to set a value for.
TAB_API_EXTRACT TAB_RESULT TabRowSetNull(
    TAB_HANDLE Row
    , int columnNumber
);

/// Sets the specified column in the row to a 32-bit unsigned integer value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param value The 32-bit integer value.
TAB_API_EXTRACT TAB_RESULT TabRowSetInteger(
    TAB_HANDLE Row
    , int columnNumber
    , int value
);

/// Sets the specified column in the row to a 64-bit unsigned integer value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param value The 64-bit integer value.
TAB_API_EXTRACT TAB_RESULT TabRowSetLongInteger(
    TAB_HANDLE Row
    , int columnNumber
    , int64_t value
);

/// Sets the specified column in the row to a double value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param value The double value.
TAB_API_EXTRACT TAB_RESULT TabRowSetDouble(
    TAB_HANDLE Row
    , int columnNumber
    , double value
);

/// Sets the specified column in the row to a Boolean value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param value True or false.
TAB_API_EXTRACT TAB_RESULT TabRowSetBoolean(
    TAB_HANDLE Row
    , int columnNumber
    , int value
);

/// Sets the specified column in the row to a string value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param value The string value.
TAB_API_EXTRACT TAB_RESULT TabRowSetString(
    TAB_HANDLE Row
    , int columnNumber
    , TableauString value
);

/// Sets the specified column in the row to a string value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param value The string value.
TAB_API_EXTRACT TAB_RESULT TabRowSetCharString(
    TAB_HANDLE Row
    , int columnNumber
    , TableauCharString value
);

/// Sets the specified column in the row to a date value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param year The year.
/// @param month The month.
/// @param day The day.
TAB_API_EXTRACT TAB_RESULT TabRowSetDate(
    TAB_HANDLE Row
    , int columnNumber
    , int year
    , int month
    , int day
);

/// Sets the specified column in the row to a date-time value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param year The year.
/// @param month The month.
/// @param day The day.
/// @param hour The hour.
/// @param min The minute.
/// @param sec The second.
/// @param frac The fraction of a second as one tenth of a millisecond (1/10000).
TAB_API_EXTRACT TAB_RESULT TabRowSetDateTime(
    TAB_HANDLE Row
    , int columnNumber
    , int year
    , int month
    , int day
    , int hour
    , int min
    , int sec
    , int frac
);

/// Sets the specified column in the row to a duration value (time span).
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param day The number of days.
/// @param hour The number of hours.
/// @param minute The number of minutes.
/// @param second The number of seconds.
/// @param frac The fraction of a second as one tenth of a millisecond (1/10000).
TAB_API_EXTRACT TAB_RESULT TabRowSetDuration(
    TAB_HANDLE Row
    , int columnNumber
    , int day
    , int hour
    , int minute
    , int second
    , int frac
);

/// Sets the specified column in the row to a geospatial value.
/// @param columnNumber The column number (zero-based) to set a value for.
/// @param value The spatial value (as a charString).
TAB_API_EXTRACT TAB_RESULT TabRowSetSpatial(
    TAB_HANDLE Row
    , int columnNumber
    , TableauCharString value
);


/*------------------------------------------------------------------------
  SECTION
  Table

  Represents a data table in the extract.

  ------------------------------------------------------------------------*/

/// Queues a row for insertion into the table. This method might insert a set of buffered rows.
/// @param row The row to insert.
TAB_API_EXTRACT TAB_RESULT TabTableInsert(
    TAB_HANDLE Table
    , TAB_HANDLE row
);

/// Gets the table's schema.
/// @param retval A copy of the table's schema, which must be closed.
TAB_API_EXTRACT TAB_RESULT TabTableGetTableDefinition(
    TAB_HANDLE Table
    , TAB_HANDLE* retval
);


/*------------------------------------------------------------------------
  SECTION
  Extract

  Represents a Tableau Data Engine database.

  ------------------------------------------------------------------------*/

/// Initializes an extract object using a file system path and file name. If the extract file already exists, this method opens the extract. If the file does not already exist, the method initializes a new extract. You must explicily close this object in order to save the extract to disk and releases its resources.
/// @param path The path and file name of the extract file to create or open. The path must include the ".tde" extension.
TAB_API_EXTRACT TAB_RESULT TabExtractCreate(
    TAB_HANDLE *handle
    , TableauString path
);

/// Closes the extract and any open tables that it contains. You must call this method in order to save the extract to a .tde file and to release its resources.
TAB_API_EXTRACT TAB_RESULT TabExtractClose(TAB_HANDLE handle);

/// Adds a table to the extract.
/// @param name The name of the table to add. Currently, this method can only add a table named "Extract".
/// @param tableDefinition The schema of the new table.
/// @param retval A reference to the table.
TAB_API_EXTRACT TAB_RESULT TabExtractAddTable(
    TAB_HANDLE Extract
    , TableauString name
    , TAB_HANDLE tableDefinition
    , TAB_HANDLE* retval
);

/// Opens the specified table in the extract.
/// @param name The name of the table to open. Currently, this method can only open a table named "Extract".
/// @param retval A reference to the table.
TAB_API_EXTRACT TAB_RESULT TabExtractOpenTable(
    TAB_HANDLE Extract
    , TableauString name
    , TAB_HANDLE* retval
);

/// Deterrmines whether the specified table exists in the extract.
/// @param name The name of the table.
/// @param retval True if the specified table exists; otherwise, false.
TAB_API_EXTRACT TAB_RESULT TabExtractHasTable(
    TAB_HANDLE Extract
    , TableauString name
    , int* retval
);


/*------------------------------------------------------------------------
  SECTION
  ExtractAPI

  Provides management functions for the Extract API.

  ------------------------------------------------------------------------*/

/// Initializes the Extract API. Calling this method is optional. The call initializes logging in the TableauSDKExtract.log file. If you call this method, you must call it before calling any other method for the Extract API.
TAB_API_EXTRACT TAB_RESULT TabExtractAPIInitialize(
);

/// Shuts down the Extract API. This call is required only if you previously called the Initialize method.
TAB_API_EXTRACT TAB_RESULT TabExtractAPICleanup(
);



#ifdef __cplusplus
}
#endif


#endif // TableauExtract_H
