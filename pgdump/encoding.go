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
	PGEncEUCJP    = 1
	PGEncEUCCN    = 2
	PGEncEUCKR    = 3
	PGEncEUCTW    = 4
	PGEncUTF8     = 6
	PGEncLATIN1   = 8
	PGEncLATIN2   = 9
	PGEncLATIN3   = 10
	PGEncLATIN4   = 11
	PGEncLATIN5   = 12
	PGEncWIN1256  = 18
	PGEncWIN866   = 20
	PGEncKOI8R    = 22
	PGEncWIN1251  = 23
	PGEncWIN1252  = 24
	PGEncISO8859_5 = 25
	PGEncISO8859_6 = 26
	PGEncISO8859_7 = 27
	PGEncISO8859_8 = 28
	PGEncWIN1250  = 29
	PGEncWIN1253  = 30
	PGEncWIN1254  = 31
	PGEncWIN1255  = 32
	PGEncWIN1257  = 33
	PGEncKOI8U    = 34
	PGEncSJIS     = 35
	PGEncBIG5     = 36
	PGEncGBK      = 37
	PGEncUHC      = 38
	PGEncGB18030  = 39
)

// pgEncodingToDecoder maps PostgreSQL encoding IDs to Go text decoders
func pgEncodingToDecoder(enc int) *encoding.Decoder {
	switch enc {
	case PGEncSQLASCII, PGEncUTF8:
		return nil // no conversion needed
	case PGEncEUCJP:
		return japanese.EUCJP.NewDecoder()
	case PGEncEUCCN:
		return simplifiedchinese.HZGB2312.NewDecoder()
	case PGEncEUCKR:
		return korean.EUCKR.NewDecoder()
	case PGEncEUCTW:
		return traditionalchinese.Big5.NewDecoder() // closest available
	case PGEncLATIN1:
		return charmap.ISO8859_1.NewDecoder()
	case PGEncLATIN2:
		return charmap.ISO8859_2.NewDecoder()
	case PGEncLATIN3:
		return charmap.ISO8859_3.NewDecoder()
	case PGEncLATIN4:
		return charmap.ISO8859_4.NewDecoder()
	case PGEncLATIN5:
		return charmap.ISO8859_9.NewDecoder()
	case PGEncWIN1250:
		return charmap.Windows1250.NewDecoder()
	case PGEncWIN1251:
		return charmap.Windows1251.NewDecoder()
	case PGEncWIN1252:
		return charmap.Windows1252.NewDecoder()
	case PGEncWIN1253:
		return charmap.Windows1253.NewDecoder()
	case PGEncWIN1254:
		return charmap.Windows1254.NewDecoder()
	case PGEncWIN1255:
		return charmap.Windows1255.NewDecoder()
	case PGEncWIN1256:
		return charmap.Windows1256.NewDecoder()
	case PGEncWIN1257:
		return charmap.Windows1257.NewDecoder()
	case PGEncISO8859_5:
		return charmap.ISO8859_5.NewDecoder()
	case PGEncISO8859_6:
		return charmap.ISO8859_6.NewDecoder()
	case PGEncISO8859_7:
		return charmap.ISO8859_7.NewDecoder()
	case PGEncISO8859_8:
		return charmap.ISO8859_8.NewDecoder()
	case PGEncKOI8R:
		return charmap.KOI8R.NewDecoder()
	case PGEncKOI8U:
		return charmap.KOI8U.NewDecoder()
	case PGEncWIN866:
		return charmap.CodePage866.NewDecoder()
	case PGEncSJIS:
		return japanese.ShiftJIS.NewDecoder()
	case PGEncBIG5:
		return traditionalchinese.Big5.NewDecoder()
	case PGEncGBK:
		return simplifiedchinese.GBK.NewDecoder()
	case PGEncUHC:
		return korean.EUCKR.NewDecoder() // UHC is superset of EUC-KR
	case PGEncGB18030:
		return simplifiedchinese.GB18030.NewDecoder()
	default:
		return nil
	}
}

// PGEncodingName returns the name of a PostgreSQL encoding ID
func PGEncodingName(enc int) string {
	names := map[int]string{
		0: "SQL_ASCII", 1: "EUC_JP", 2: "EUC_CN", 3: "EUC_KR",
		4: "EUC_TW", 6: "UTF8", 8: "LATIN1", 9: "LATIN2",
		10: "LATIN3", 11: "LATIN4", 12: "LATIN5", 18: "WIN1256",
		20: "WIN866", 22: "KOI8R", 23: "WIN1251", 24: "WIN1252",
		25: "ISO-8859-5", 26: "ISO-8859-6", 27: "ISO-8859-7", 28: "ISO-8859-8",
		29: "WIN1250", 30: "WIN1253", 31: "WIN1254", 32: "WIN1255",
		33: "WIN1257", 34: "KOI8U", 35: "SJIS", 36: "BIG5",
		37: "GBK", 38: "UHC", 39: "GB18030",
	}
	if name, ok := names[enc]; ok {
		return name
	}
	return "UNKNOWN"
}

// ConvertToUTF8 converts a string from a PostgreSQL encoding to UTF-8
func ConvertToUTF8(s string, enc int) string {
	decoder := pgEncodingToDecoder(enc)
	if decoder == nil {
		return s
	}
	result, err := decoder.String(s)
	if err != nil {
		return s
	}
	return result
}

// OutputEncoder returns an encoder for the given encoding name.
// Returns nil for UTF-8 or empty string (no conversion needed).
func OutputEncoder(name string) *encoding.Encoder {
	if name == "" || strings.EqualFold(name, "UTF-8") || strings.EqualFold(name, "UTF8") {
		return nil
	}
	enc := encodingByName(name)
	if enc == nil {
		return nil
	}
	return enc.NewEncoder()
}

func encodingByName(name string) encoding.Encoding {
	switch strings.ToUpper(strings.ReplaceAll(name, "-", "")) {
	case "GBK":
		return simplifiedchinese.GBK
	case "GB18030":
		return simplifiedchinese.GB18030
	case "GB2312", "EUCCN":
		return simplifiedchinese.HZGB2312
	case "BIG5":
		return traditionalchinese.Big5
	case "SHIFTJIS", "SJIS":
		return japanese.ShiftJIS
	case "EUCJP":
		return japanese.EUCJP
	case "EUCKR", "UHC":
		return korean.EUCKR
	case "LATIN1", "ISO88591":
		return charmap.ISO8859_1
	case "LATIN2", "ISO88592":
		return charmap.ISO8859_2
	case "LATIN3", "ISO88593":
		return charmap.ISO8859_3
	case "LATIN4", "ISO88594":
		return charmap.ISO8859_4
	case "LATIN5", "ISO88599":
		return charmap.ISO8859_9
	case "WIN1250", "WINDOWS1250":
		return charmap.Windows1250
	case "WIN1251", "WINDOWS1251":
		return charmap.Windows1251
	case "WIN1252", "WINDOWS1252":
		return charmap.Windows1252
	case "WIN1253", "WINDOWS1253":
		return charmap.Windows1253
	case "WIN1254", "WINDOWS1254":
		return charmap.Windows1254
	case "WIN1255", "WINDOWS1255":
		return charmap.Windows1255
	case "WIN1256", "WINDOWS1256":
		return charmap.Windows1256
	case "WIN1257", "WINDOWS1257":
		return charmap.Windows1257
	case "KOI8R":
		return charmap.KOI8R
	case "KOI8U":
		return charmap.KOI8U
	default:
		return nil
	}
}
