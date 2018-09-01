// -----------------------------------------------------------------------------
//
// This file is the copyrighted property of Tableau Software and is protected
// by registered patents and other applicable U.S. and international laws and
// regulations.
//
// Unlicensed use of the contents of this file is prohibited. Please refer to
// the NOTICES.txt file for further details.
//
// -----------------------------------------------------------------------------

#ifndef TABLEAUCOMMON_H
#define TABLEAUCOMMON_H

#include <stdint.h>
#include <wchar.h>

#if defined(_WIN32)
#  ifdef TBL_TABLEAUCOMMON_BUILD
#    define TAB_API_COMMON __declspec(dllexport)
#  else
#    define TAB_API_COMMON __declspec(dllimport)
#  endif
#else
#    define TAB_API_COMMON __attribute__ ((visibility ("default")))
#endif

typedef void*                 TAB_HANDLE;
typedef unsigned short        TableauWChar;
typedef const TableauWChar*   TableauString;
typedef const char*           TableauCharString;

#ifdef __cplusplus
extern "C" {
#endif

/*------------------------------------------------------------------------
  TAB_TYPE

  ------------------------------------------------------------------------*/

typedef int32_t TAB_TYPE;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_Integer;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_Double;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_Boolean;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_Date;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_DateTime;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_Duration;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_CharString;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_UnicodeString;
TAB_API_COMMON const extern TAB_TYPE TAB_TYPE_Spatial;

/*------------------------------------------------------------------------
  TAB_RESULT

  ------------------------------------------------------------------------*/

typedef int32_t TAB_RESULT;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_Success;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_OutOfMemory;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_PermissionDenied;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_InvalidFile;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_FileExists;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_TooManyFiles;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_FileNotFound;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_DiskFull;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_DirectoryNotEmpty;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_NoSuchDatabase;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_QueryError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_NullArgument;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_DataEngineError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_Cancelled;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_BadIndex;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_ProtocolError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_NetworkError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_InternalError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_WrongType;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_UsageError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_InvalidArgument;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_BadHandle;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_CurlError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_ServerError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_NotAuthenticated;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_BadPayload;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_InitError;
TAB_API_COMMON const extern TAB_RESULT TAB_RESULT_UnknownError;

/*------------------------------------------------------------------------
  TAB_COLLATION

  ------------------------------------------------------------------------*/

typedef int32_t TAB_COLLATION;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_Binary;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_ar;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_cs;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_cs_CI;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_cs_CI_AI;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_da;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_de;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_el;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_en_GB;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_en_US;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_en_US_CI;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_es;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_es_CI_AI;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_et;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_fi;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_fr_CA;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_fr_FR;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_fr_FR_CI_AI;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_he;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_hu;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_is;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_it;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_ja;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_ja_JIS;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_ko;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_lt;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_lv;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_nl_NL;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_nn;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_pl;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_pt_BR;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_pt_BR_CI_AI;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_pt_PT;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_root;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_ru;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_sl;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_sv_FI;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_sv_SE;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_tr;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_uk;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_vi;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_zh_Hans_CN;
TAB_API_COMMON const extern TAB_COLLATION TAB_COLLATION_zh_Hant_TW;

TAB_API_COMMON const wchar_t* TabGetLastErrorMessage();
TAB_API_COMMON void TabSetLastErrorMessage( const wchar_t* );
TAB_API_COMMON void TabShutdown();

/// Convert a wide string to a TableauString.
/// @param ws The null-terminated string to convert.
/// @param ts Buffer for null-terminated output; presumed to be large enough.
TAB_API_COMMON void ToTableauString( const wchar_t* ws, TableauWChar* ts );

/// Convert a TableauString to a wide string.
/// @param ts The null-terminated TableauString to convert.
/// @param ws Buffer for null-terminated output; presumed to be large enough.
TAB_API_COMMON void FromTableauString( const TableauString ts, wchar_t* ws );

/// Measure the length of a null-terminated Tableau String.
TAB_API_COMMON int TableauStringLength( const TableauString ts );

#ifdef __cplusplus
}
#endif

#endif // TABLEAUCOMMON_H
