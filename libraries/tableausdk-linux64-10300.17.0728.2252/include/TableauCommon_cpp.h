// -----------------------------------------------------------------------
// Copyright (c) 2012 Tableau Software, Incorporated
//                    and its licensors. All rights reserved.
// Protected by U.S. Patent 7,089,266; Patents Pending.
//
// Portions of the code
// Copyright (c) 2002 The Board of Trustees of the Leland Stanford
//                    Junior University. All rights reserved.
// -----------------------------------------------------------------------
// TableauCommon_cpp.h
// -----------------------------------------------------------------------
// WARNING: Computer generated file.  Do not hand modify.

#ifndef TableauCommon_CPP_H
#define TableauCommon_CPP_H

#include "TableauCommon.h"
#include <cerrno>
#include <memory>
#include <string>

namespace Tableau {

typedef void* TableauHandle;

/*------------------------------------------------------------------------
  Type

  ------------------------------------------------------------------------*/

enum Type
{
    Type_Integer                          = 0x0007,    // TDE_DT_SINT64
    Type_Double                           = 0x000A,    // TDE_DT_DOUBLE
    Type_Boolean                          = 0x000B,    // TDE_DT_BOOL
    Type_Date                             = 0x000C,    // TDE_DT_DATE
    Type_DateTime                         = 0x000D,    // TDE_DT_DATETIME
    Type_Duration                         = 0x000E,    // TDE_DT_DURATION
    Type_CharString                       = 0x000F,    // TDE_DT_STR
    Type_UnicodeString                    = 0x0010,    // TDE_DT_WSTR
    Type_Spatial                          = 0X0011,    // TDE_DT_SPATIAL
};

/*------------------------------------------------------------------------
  Result

  ------------------------------------------------------------------------*/

enum Result
{
    Result_Success                        = 0,         // Successful function call
    Result_OutOfMemory                    = ENOMEM,    // 
    Result_PermissionDenied               = EACCES,    // 
    Result_InvalidFile                    = EBADF,     // 
    Result_FileExists                     = EEXIST,    // 
    Result_TooManyFiles                   = EMFILE,    // 
    Result_FileNotFound                   = ENOENT,    // 
    Result_DiskFull                       = ENOSPC,    // 
    Result_DirectoryNotEmpty              = ENOTEMPTY, // 
    Result_NoSuchDatabase                 = 201,       // Data Engine errors start at 200.
    Result_QueryError                     = 202,       // 
    Result_NullArgument                   = 203,       // 
    Result_DataEngineError                = 204,       // 
    Result_Cancelled                      = 205,       // 
    Result_BadIndex                       = 206,       // 
    Result_ProtocolError                  = 207,       // 
    Result_NetworkError                   = 208,       // 
    Result_InternalError                  = 300,       // 300+: other error codes
    Result_WrongType                      = 301,       // 
    Result_UsageError                     = 302,       // 
    Result_InvalidArgument                = 303,       // 
    Result_BadHandle                      = 304,       // 
    Result_CurlError                      = 400,       // 400+: Server Client error codes
    Result_ServerError                    = 401,       // 
    Result_NotAuthenticated               = 402,       // 
    Result_BadPayload                     = 403,       // 
    Result_InitError                      = 404,       // 
    Result_UnknownError                   = 999,       // 
};

/*------------------------------------------------------------------------
  Collation

  ------------------------------------------------------------------------*/

enum Collation
{
    Collation_Binary                      = 0,         // Internal binary representation
    Collation_ar                          = 1,         // Arabic
    Collation_cs                          = 2,         // Czech
    Collation_cs_CI                       = 3,         // Czech (Case Insensitive)
    Collation_cs_CI_AI                    = 4,         // Czech (Case/Accent Insensitive
    Collation_da                          = 5,         // Danish
    Collation_de                          = 6,         // German
    Collation_el                          = 7,         // Greek
    Collation_en_GB                       = 8,         // English (Great Britain)
    Collation_en_US                       = 9,         // English (US)
    Collation_en_US_CI                    = 10,        // English (US, Case Insensitive)
    Collation_es                          = 11,        // Spanish
    Collation_es_CI_AI                    = 12,        // Spanish (Case/Accent Insensitive)
    Collation_et                          = 13,        // Estonian
    Collation_fi                          = 14,        // Finnish
    Collation_fr_CA                       = 15,        // French (Canada)
    Collation_fr_FR                       = 16,        // French (France)
    Collation_fr_FR_CI_AI                 = 17,        // French (France, Case/Accent Insensitive)
    Collation_he                          = 18,        // Hebrew
    Collation_hu                          = 19,        // Hungarian
    Collation_is                          = 20,        // Icelandic
    Collation_it                          = 21,        // Italian
    Collation_ja                          = 22,        // Japanese
    Collation_ja_JIS                      = 23,        // Japanese (JIS)
    Collation_ko                          = 24,        // Korean
    Collation_lt                          = 25,        // Lithuanian
    Collation_lv                          = 26,        // Latvian
    Collation_nl_NL                       = 27,        // Dutch (Netherlands)
    Collation_nn                          = 28,        // Norwegian
    Collation_pl                          = 29,        // Polish
    Collation_pt_BR                       = 30,        // Portuguese (Brazil)
    Collation_pt_BR_CI_AI                 = 31,        // Portuguese (Brazil Case/Accent Insensitive)
    Collation_pt_PT                       = 32,        // Portuguese (Portugal)
    Collation_root                        = 33,        // Root
    Collation_ru                          = 34,        // Russian
    Collation_sl                          = 35,        // Slovenian
    Collation_sv_FI                       = 36,        // Swedish (Finland)
    Collation_sv_SE                       = 37,        // Swedish (Sweden)
    Collation_tr                          = 38,        // Turkish
    Collation_uk                          = 39,        // Ukrainian
    Collation_vi                          = 40,        // Vietnamese
    Collation_zh_Hans_CN                  = 41,        // Chinese (Simplified, China)
    Collation_zh_Hant_TW                  = 42,        // Chinese (Traditional, Taiwan)
};


/*------------------------------------------------------------------------
  CLASS
  TableauException

  A general exception originating in Tableau code.

  ------------------------------------------------------------------------*/
class TableauException {
  public:
    TableauException( const TAB_RESULT r, const std::wstring m ) : m_result(r), m_message(m) {}

    const TAB_RESULT GetResultCode() const { return m_result; }
    const std::wstring GetMessage() const { return m_message; }

  private:
    const TAB_RESULT m_result;
    const std::wstring m_message;
};

namespace {

    std::basic_string<TableauWChar> MakeTableauString( const wchar_t* s )
    {
        const int len = static_cast<int>( wcslen(s) );
        TableauWChar* ts = new TableauWChar[len + 1];

        ToTableauString( s, ts );
        std::basic_string<TableauWChar> ret( ts );
        delete [] ts;

        return ret;
    }

    std::wstring ToStdString( TableauString s )
    {
        const int nChars = TableauStringLength( s ) + 1;

        wchar_t* ws = new wchar_t[nChars];

        FromTableauString( s, ws );
        std::wstring str( ws );
        delete [] ws;

        return str;
    }
}

} // namespace Tableau
#endif // TableauCommon_CPP_H
