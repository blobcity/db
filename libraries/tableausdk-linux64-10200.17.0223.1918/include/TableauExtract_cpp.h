// -----------------------------------------------------------------------
// Copyright (c) 2012 Tableau Software, Incorporated
//                    and its licensors. All rights reserved.
// Protected by U.S. Patent 7,089,266; Patents Pending.
//
// Portions of the code
// Copyright (c) 2002 The Board of Trustees of the Leland Stanford
//                    Junior University. All rights reserved.
// -----------------------------------------------------------------------
// TableauExtract_cpp.h
// -----------------------------------------------------------------------
// WARNING: Computer generated file.  Do not hand modify.

#ifndef TableauExtract_CPP_H
#define TableauExtract_CPP_H

#include "TableauExtract.h"
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
  TableDefinition

  Represents the schema for a table in a Tableau data extract. The schema consists of a collection of column definitions, or more specifically name/type pairs.

  ------------------------------------------------------------------------*/

class TableDefinition
{
  public:
    /// Initializes a new instance of the TableDefinition class.
    TableDefinition(
    );

    /// Closes the TableDefinition object and frees associated memory.
    void Close();

    /// Calls Close().
    ~TableDefinition();

    /// Returns the default collation for the table definition. The default is Collation.BINARY (0). You can change the default collation by calling <b>SetDefaultCollaction</b>.
   /// @return The default collation.
    Collation
    GetDefaultCollation(
    );

    /// Sets the default collation for new string columns.
    /// @param collation The default collation for new string columns.
    void
    SetDefaultCollation(
        Collation collation
    );

    /// Adds a column to the table definition. The order in which columns are added specifies their column number. String columns are defined with the current default collation.
    /// @param name The name of the column to add.
    /// @param type The data type of the column to add.
    void
    AddColumn(
        std::wstring name,
        Type type
    );

    /// Adds a column that has the specified collation.
    /// @param name The name of the column to add.
    /// @param type The data type of the column to add.
    /// @param collation For string columns, the collation to use. For other types of columns, this value is ignored.
    void
    AddColumnWithCollation(
        std::wstring name,
        Type type,
        Collation collation
    );

    /// Returns the number of columns in the table definition.
   /// @return The number of columns.
    int
    GetColumnCount(
    );

    /// Returns the name of the specified column.
    /// @param columnNumber The column number (zero-based) to return the name for.
   /// @return The column name.
    std::wstring
    GetColumnName(
        int columnNumber
    );

    /// Returns the data type of the specified column.
    /// @param columnNumber The column number (zero-based) to return the name for.
   /// @return The column data type.
    Type
    GetColumnType(
        int columnNumber
    );

    /// Returns the collation of the specified column.
    /// @param columnNumber The column number (zero-based) to return the collation for.
   /// @return The column collation.
    Collation
    GetColumnCollation(
        int columnNumber
    );


  private:
    TAB_HANDLE m_handle;

    // Forbidden:
    TableDefinition( const TableDefinition& );
    TableDefinition& operator=( const TableDefinition& );

    friend class Row;
    friend class Extract;
    friend class Table;
};

/*------------------------------------------------------------------------
  CLASS
  Row

  Defines a row to be inserted into a table in an extract. The row is structured as a tuple.

  ------------------------------------------------------------------------*/

class Row
{
  public:
    /// Initializes a new instance of the Row class. This method creates an empty row that has the specified schema.
    /// @param tableDefinition The schema to use.
    Row(
        TableDefinition& tableDefinition
    );

    /// Closes the row and frees associated resources.
    void Close();

    /// Calls Close().
    ~Row();

    /// Sets the specified column in the row to null.
    /// @param columnNumber The column number (zero-based) to set a value for.
    void
    SetNull(
        int columnNumber
    );

    /// Sets the specified column in the row to a 32-bit unsigned integer value.
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param value The 32-bit integer value.
    void
    SetInteger(
        int columnNumber,
        int value
    );

    /// Sets the specified column in the row to a 64-bit unsigned integer value.
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param value The 64-bit integer value.
    void
    SetLongInteger(
        int columnNumber,
        int64_t value
    );

    /// Sets the specified column in the row to a double value.
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param value The double value.
    void
    SetDouble(
        int columnNumber,
        double value
    );

    /// Sets the specified column in the row to a Boolean value.
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param value True or false.
    void
    SetBoolean(
        int columnNumber,
        bool value
    );

    /// Sets the specified column in the row to a string value.
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param value The string value.
    void
    SetString(
        int columnNumber,
        std::wstring value
    );

    /// Sets the specified column in the row to a string value.
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param value The string value.
    void
    SetCharString(
        int columnNumber,
        std::string value
    );

    /// Sets the specified column in the row to a date value.
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param year The year.
    /// @param month The month.
    /// @param day The day.
    void
    SetDate(
        int columnNumber,
        int year,
        int month,
        int day
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
    void
    SetDateTime(
        int columnNumber,
        int year,
        int month,
        int day,
        int hour,
        int min,
        int sec,
        int frac
    );

    /// Sets the specified column in the row to a duration value (time span).
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param day The number of days.
    /// @param hour The number of hours.
    /// @param minute The number of minutes.
    /// @param second The number of seconds.
    /// @param frac The fraction of a second as one tenth of a millisecond (1/10000).
    void
    SetDuration(
        int columnNumber,
        int day,
        int hour,
        int minute,
        int second,
        int frac
    );

    /// Sets the specified column in the row to a geospatial value.
    /// @param columnNumber The column number (zero-based) to set a value for.
    /// @param value The spatial value (as a charString).
    void
    SetSpatial(
        int columnNumber,
        std::string value
    );


  private:
    TAB_HANDLE m_handle;

    // Forbidden:
    Row( const Row& );
    Row& operator=( const Row& );

    friend class Table;
};

/*------------------------------------------------------------------------
  CLASS
  Table

  Represents a data table in the extract.

  ------------------------------------------------------------------------*/

class Table
{
  public:
    /// Queues a row for insertion into the table. This method might insert a set of buffered rows.
    /// @param row The row to insert.
    void
    Insert(
        Row& row
    );

    /// Gets the table's schema.
   /// @return A copy of the table's schema, which must be closed.
    std::shared_ptr<TableDefinition>
    GetTableDefinition(
    );


  private:
    TAB_HANDLE m_handle;

    Table() : m_handle(nullptr) {}

    // Forbidden:
    Table( const Table& );
    Table& operator=( const Table& );

    friend class Extract;
};

/*------------------------------------------------------------------------
  CLASS
  Extract

  Represents a Tableau Data Engine database.

  ------------------------------------------------------------------------*/

class Extract
{
  public:
    /// Initializes an extract object using a file system path and file name. If the extract file already exists, this method opens the extract. If the file does not already exist, the method initializes a new extract. You must explicily close this object in order to save the extract to disk and releases its resources.
    /// @param path The path and file name of the extract file to create or open. The path must include the ".tde" extension.
    Extract(
        std::wstring path
    );

    /// Closes the extract and any open tables that it contains. You must call this method in order to save the extract to a .tde file and to release its resources.
    void Close();

    /// Calls Close().
    ~Extract();

    /// Adds a table to the extract.
    /// @param name The name of the table to add. Currently, this method can only add a table named "Extract".
    /// @param tableDefinition The schema of the new table.
   /// @return A reference to the table.
    std::shared_ptr<Table>
    AddTable(
        std::wstring name,
        TableDefinition& tableDefinition
    );

    /// Opens the specified table in the extract.
    /// @param name The name of the table to open. Currently, this method can only open a table named "Extract".
   /// @return A reference to the table.
    std::shared_ptr<Table>
    OpenTable(
        std::wstring name
    );

    /// Deterrmines whether the specified table exists in the extract.
    /// @param name The name of the table.
   /// @return True if the specified table exists; otherwise, false.
    bool
    HasTable(
        std::wstring name
    );


  private:
    TAB_HANDLE m_handle;

    // Forbidden:
    Extract( const Extract& );
    Extract& operator=( const Extract& );

};

/*------------------------------------------------------------------------
  CLASS
  ExtractAPI

  Provides management functions for the Extract API.

  ------------------------------------------------------------------------*/

class ExtractAPI
{
  public:
    /// Initializes the Extract API. Calling this method is optional. The call initializes logging in the TableauSDKExtract.log file. If you call this method, you must call it before calling any other method for the Extract API.
    static
    void
    Initialize(
    );

    /// Shuts down the Extract API. This call is required only if you previously called the Initialize method.
    static
    void
    Cleanup(
    );


  private:
    // Forbidden:
    ExtractAPI( const ExtractAPI& );
    ExtractAPI& operator=( const ExtractAPI& );

};




// -----------------------------------------------------------------------
// TableDefinition methods
// -----------------------------------------------------------------------

// Initializes a new instance of the TableDefinition class.
inline TableDefinition::TableDefinition(
)
{
    TAB_RESULT result = TabTableDefinitionCreate(
        &m_handle
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Closes the TableDefinition object and frees associated memory.
inline TableDefinition::~TableDefinition()
{
    Close();
}

inline void TableDefinition::Close()
{
    if ( m_handle != nullptr ) {
        TAB_RESULT result = TabTableDefinitionClose( m_handle );
        m_handle = nullptr;

        if ( result != TAB_RESULT_Success )
            throw TableauException( result, TabGetLastErrorMessage() );
    }
}

// Returns the default collation for the table definition. The default is Collation.BINARY (0). You can change the default collation by calling <b>SetDefaultCollaction</b>.
inline Collation
TableDefinition::GetDefaultCollation(
)
{
    TAB_COLLATION retval;
    TAB_RESULT result = TabTableDefinitionGetDefaultCollation(m_handle
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    return static_cast< Collation >( retval );
}

// Sets the default collation for new string columns.
inline void
TableDefinition::SetDefaultCollation(
    Collation collation
)
{
    TAB_RESULT result = TabTableDefinitionSetDefaultCollation(m_handle
        , collation
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Adds a column to the table definition. The order in which columns are added specifies their column number. String columns are defined with the current default collation.
inline void
TableDefinition::AddColumn(
    std::wstring name,
    Type type
)
{
    TAB_RESULT result = TabTableDefinitionAddColumn(m_handle
        , MakeTableauString(name.c_str()).c_str()
        , type
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Adds a column that has the specified collation.
inline void
TableDefinition::AddColumnWithCollation(
    std::wstring name,
    Type type,
    Collation collation
)
{
    TAB_RESULT result = TabTableDefinitionAddColumnWithCollation(m_handle
        , MakeTableauString(name.c_str()).c_str()
        , type
        , collation
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Returns the number of columns in the table definition.
inline int
TableDefinition::GetColumnCount(
)
{
    int retval;
    TAB_RESULT result = TabTableDefinitionGetColumnCount(m_handle
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    return static_cast< int >( retval );
}

// Returns the name of the specified column.
inline std::wstring
TableDefinition::GetColumnName(
    int columnNumber
)
{
    TableauString retval;
    TAB_RESULT result = TabTableDefinitionGetColumnName(m_handle
        , columnNumber
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    return ToStdString( retval );
}

// Returns the data type of the specified column.
inline Type
TableDefinition::GetColumnType(
    int columnNumber
)
{
    TAB_TYPE retval;
    TAB_RESULT result = TabTableDefinitionGetColumnType(m_handle
        , columnNumber
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    return static_cast< Type >( retval );
}

// Returns the collation of the specified column.
inline Collation
TableDefinition::GetColumnCollation(
    int columnNumber
)
{
    TAB_COLLATION retval;
    TAB_RESULT result = TabTableDefinitionGetColumnCollation(m_handle
        , columnNumber
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    return static_cast< Collation >( retval );
}



// -----------------------------------------------------------------------
// Row methods
// -----------------------------------------------------------------------

// Initializes a new instance of the Row class. This method creates an empty row that has the specified schema.
inline Row::Row(
    TableDefinition& tableDefinition
)
{
    TAB_RESULT result = TabRowCreate(
        &m_handle
        , tableDefinition.m_handle
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Closes the row and frees associated resources.
inline Row::~Row()
{
    Close();
}

inline void Row::Close()
{
    if ( m_handle != nullptr ) {
        TAB_RESULT result = TabRowClose( m_handle );
        m_handle = nullptr;

        if ( result != TAB_RESULT_Success )
            throw TableauException( result, TabGetLastErrorMessage() );
    }
}

// Sets the specified column in the row to null.
inline void
Row::SetNull(
    int columnNumber
)
{
    TAB_RESULT result = TabRowSetNull(m_handle
        , columnNumber
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a 32-bit unsigned integer value.
inline void
Row::SetInteger(
    int columnNumber,
    int value
)
{
    TAB_RESULT result = TabRowSetInteger(m_handle
        , columnNumber
        , value
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a 64-bit unsigned integer value.
inline void
Row::SetLongInteger(
    int columnNumber,
    int64_t value
)
{
    TAB_RESULT result = TabRowSetLongInteger(m_handle
        , columnNumber
        , value
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a double value.
inline void
Row::SetDouble(
    int columnNumber,
    double value
)
{
    TAB_RESULT result = TabRowSetDouble(m_handle
        , columnNumber
        , value
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a Boolean value.
inline void
Row::SetBoolean(
    int columnNumber,
    bool value
)
{
    TAB_RESULT result = TabRowSetBoolean(m_handle
        , columnNumber
        , value
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a string value.
inline void
Row::SetString(
    int columnNumber,
    std::wstring value
)
{
    TAB_RESULT result = TabRowSetString(m_handle
        , columnNumber
        , MakeTableauString(value.c_str()).c_str()
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a string value.
inline void
Row::SetCharString(
    int columnNumber,
    std::string value
)
{
    TAB_RESULT result = TabRowSetCharString(m_handle
        , columnNumber
        , value.c_str()
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a date value.
inline void
Row::SetDate(
    int columnNumber,
    int year,
    int month,
    int day
)
{
    TAB_RESULT result = TabRowSetDate(m_handle
        , columnNumber
        , year
        , month
        , day
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a date-time value.
inline void
Row::SetDateTime(
    int columnNumber,
    int year,
    int month,
    int day,
    int hour,
    int min,
    int sec,
    int frac
)
{
    TAB_RESULT result = TabRowSetDateTime(m_handle
        , columnNumber
        , year
        , month
        , day
        , hour
        , min
        , sec
        , frac
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a duration value (time span).
inline void
Row::SetDuration(
    int columnNumber,
    int day,
    int hour,
    int minute,
    int second,
    int frac
)
{
    TAB_RESULT result = TabRowSetDuration(m_handle
        , columnNumber
        , day
        , hour
        , minute
        , second
        , frac
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Sets the specified column in the row to a geospatial value.
inline void
Row::SetSpatial(
    int columnNumber,
    std::string value
)
{
    TAB_RESULT result = TabRowSetSpatial(m_handle
        , columnNumber
        , value.c_str()
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}



// -----------------------------------------------------------------------
// Table methods
// -----------------------------------------------------------------------

// Queues a row for insertion into the table. This method might insert a set of buffered rows.
inline void
Table::Insert(
    Row& row
)
{
    TAB_RESULT result = TabTableInsert(m_handle
        , row.m_handle
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Gets the table's schema.
inline std::shared_ptr<TableDefinition>
Table::GetTableDefinition(
)
{
    TAB_HANDLE retval;
    TAB_RESULT result = TabTableGetTableDefinition(m_handle
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    std::shared_ptr<TableDefinition> ret = std::shared_ptr<TableDefinition>(new TableDefinition);
    ret->m_handle = retval;
    return ret;
}



// -----------------------------------------------------------------------
// Extract methods
// -----------------------------------------------------------------------

// Initializes an extract object using a file system path and file name. If the extract file already exists, this method opens the extract. If the file does not already exist, the method initializes a new extract. You must explicily close this object in order to save the extract to disk and releases its resources.
inline Extract::Extract(
    std::wstring path
)
{
    TAB_RESULT result = TabExtractCreate(
        &m_handle
        , MakeTableauString(path.c_str()).c_str()
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Closes the extract and any open tables that it contains. You must call this method in order to save the extract to a .tde file and to release its resources.
inline Extract::~Extract()
{
    Close();
}

inline void Extract::Close()
{
    if ( m_handle != nullptr ) {
        TAB_RESULT result = TabExtractClose( m_handle );
        m_handle = nullptr;

        if ( result != TAB_RESULT_Success )
            throw TableauException( result, TabGetLastErrorMessage() );
    }
}

// Adds a table to the extract.
inline std::shared_ptr<Table>
Extract::AddTable(
    std::wstring name,
    TableDefinition& tableDefinition
)
{
    TAB_HANDLE retval;
    TAB_RESULT result = TabExtractAddTable(m_handle
        , MakeTableauString(name.c_str()).c_str()
        , tableDefinition.m_handle
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    std::shared_ptr<Table> ret = std::shared_ptr<Table>(new Table);
    ret->m_handle = retval;
    return ret;
}

// Opens the specified table in the extract.
inline std::shared_ptr<Table>
Extract::OpenTable(
    std::wstring name
)
{
    TAB_HANDLE retval;
    TAB_RESULT result = TabExtractOpenTable(m_handle
        , MakeTableauString(name.c_str()).c_str()
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    std::shared_ptr<Table> ret = std::shared_ptr<Table>(new Table);
    ret->m_handle = retval;
    return ret;
}

// Deterrmines whether the specified table exists in the extract.
inline bool
Extract::HasTable(
    std::wstring name
)
{
    int retval;
    TAB_RESULT result = TabExtractHasTable(m_handle
        , MakeTableauString(name.c_str()).c_str()
        , &retval
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );

    return retval != 0;
}



// -----------------------------------------------------------------------
// ExtractAPI methods
// -----------------------------------------------------------------------

// Initializes the Extract API. Calling this method is optional. The call initializes logging in the TableauSDKExtract.log file. If you call this method, you must call it before calling any other method for the Extract API.
inline void
ExtractAPI::Initialize(
)
{
    TAB_RESULT result = TabExtractAPIInitialize(
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

// Shuts down the Extract API. This call is required only if you previously called the Initialize method.
inline void
ExtractAPI::Cleanup(
)
{
    TAB_RESULT result = TabExtractAPICleanup(
    );

    if ( result != TAB_RESULT_Success )
        throw TableauException( result, TabGetLastErrorMessage() );
}

} // namespace Tableau
#endif // TableauExtract_CPP_H
