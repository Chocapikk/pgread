package pgdump

import (
	"strings"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

// PostgreSQL encoding IDs from src/include/mb/pg_wchar.h
const (
	PGEncSQLASCII = 0
	PGEncUTF8     = 6
)

type pgEncEntry struct {
	name     string
	aliases  []string
	encoding encoding.Encoding
}

var pgEncodings = map[int]pgEncEntry{
	1:  {"EUC_JP", []string{"EUCJP"}, japanese.EUCJP},
	2:  {"EUC_CN", []string{"EUCCN", "GB2312"}, simplifiedchinese.HZGB2312},
	3:  {"EUC_KR", []string{"EUCKR"}, korean.EUCKR},
	4:  {"EUC_TW", nil, traditionalchinese.Big5},
	8:  {"LATIN1", []string{"ISO88591"}, charmap.ISO8859_1},
	9:  {"LATIN2", []string{"ISO88592"}, charmap.ISO8859_2},
	10: {"LATIN3", []string{"ISO88593"}, charmap.ISO8859_3},
	11: {"LATIN4", []string{"ISO88594"}, charmap.ISO8859_4},
	12: {"LATIN5", []string{"ISO88599"}, charmap.ISO8859_9},
	18: {"WIN1256", []string{"WINDOWS1256"}, charmap.Windows1256},
	20: {"WIN866", nil, charmap.CodePage866},
	22: {"KOI8R", nil, charmap.KOI8R},
	23: {"WIN1251", []string{"WINDOWS1251"}, charmap.Windows1251},
	24: {"WIN1252", []string{"WINDOWS1252"}, charmap.Windows1252},
	25: {"ISO88595", nil, charmap.ISO8859_5},
	26: {"ISO88596", nil, charmap.ISO8859_6},
	27: {"ISO88597", nil, charmap.ISO8859_7},
	28: {"ISO88598", nil, charmap.ISO8859_8},
	29: {"WIN1250", []string{"WINDOWS1250"}, charmap.Windows1250},
	30: {"WIN1253", []string{"WINDOWS1253"}, charmap.Windows1253},
	31: {"WIN1254", []string{"WINDOWS1254"}, charmap.Windows1254},
	32: {"WIN1255", []string{"WINDOWS1255"}, charmap.Windows1255},
	33: {"WIN1257", []string{"WINDOWS1257"}, charmap.Windows1257},
	34: {"KOI8U", nil, charmap.KOI8U},
	35: {"SJIS", []string{"SHIFTJIS"}, japanese.ShiftJIS},
	36: {"BIG5", nil, traditionalchinese.Big5},
	37: {"GBK", nil, simplifiedchinese.GBK},
	38: {"UHC", nil, korean.EUCKR},
	39: {"GB18030", nil, simplifiedchinese.GB18030},
}

func pgEncodingToDecoder(enc int) *encoding.Decoder {
	if e, ok := pgEncodings[enc]; ok {
		return e.encoding.NewDecoder()
	}
	return nil
}

// PGEncodingName returns the name of a PostgreSQL encoding ID
func PGEncodingName(enc int) string {
	if e, ok := pgEncodings[enc]; ok {
		return e.name
	}
	if enc == PGEncSQLASCII {
		return "SQL_ASCII"
	}
	if enc == PGEncUTF8 {
		return "UTF8"
	}
	return "UNKNOWN"
}

// ConvertToUTF8 converts a string from a PostgreSQL encoding to UTF-8
func ConvertToUTF8(s string, enc int) string {
	decoder := pgEncodingToDecoder(enc)
	if decoder == nil {
		return s
	}
	if result, err := decoder.String(s); err == nil {
		return result
	}
	return s
}

// OutputEncoder returns an encoder for the given encoding name.
// Returns nil for UTF-8 or empty string (no conversion needed).
func OutputEncoder(name string) *encoding.Encoder {
	if name == "" || strings.EqualFold(name, "UTF-8") || strings.EqualFold(name, "UTF8") {
		return nil
	}
	norm := strings.ToUpper(strings.ReplaceAll(name, "-", ""))
	for _, e := range pgEncodings {
		if e.name == norm {
			return e.encoding.NewEncoder()
		}
		for _, alias := range e.aliases {
			if alias == norm {
				return e.encoding.NewEncoder()
			}
		}
	}
	return nil
}
