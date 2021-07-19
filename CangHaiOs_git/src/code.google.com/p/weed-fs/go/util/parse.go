package util

import (
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	LocalTimeFormat = "Mon,2 Jan 2006 15:04:05"
)

func ParseInt(text string, defaultValue int) int {
	count, parseError := strconv.ParseInt(text, 10, 64)
	if parseError != nil {
		if len(text) > 0 {
			return 0
		}
		return defaultValue
	}
	return int(count)
}

func ParseInt64(text string, defaultValue int64) int64 {
	num, parseError := strconv.ParseInt(text, 10, 64)
	if parseError != nil {
		return defaultValue
	}

	return int64(num)
}

//Parse digital nano second time string into raw time
func ParseUnixNano(nanoSec string) (gmtDate string) {
	count := ParseInt64(nanoSec, -1)
	if count <= 0 {
		return ""
	}
	nanoDate := time.Unix(count/int64(1000000000), 0)
	gmtDate = nanoDate.Format(LocalTimeFormat)
	return
}

func ParseUnixNanoToTime(nanoSec string) (nanoDate time.Time) {
	count := ParseInt64(nanoSec, -1)
	if count <= 0 {
		return nanoDate
	}
	nanoDate = time.Unix(count/int64(1000000000), 0)
	return
}

//Parse digital  secondd time string into gmt time by senconds
func ParseUnixSec(sec string)(secs int64){
	count := ParseInt64(sec, -1)
	if count <= 0 {
		return 0
	}
	unixDate := time.Unix(count, 0)
	gmtDate := unixDate.Add(time.Duration(-8 * time.Hour))
	return gmtDate.Unix()
}

//Parse digital  secondd time string into gmt time
func ParseUnix(sec string) (date string) {
	count := ParseInt64(sec, -1)
	if count <= 0 {
		return ""
	}
	unixDate := time.Unix(count, 0)
	gmtDate := unixDate.Add(time.Duration(-8 * time.Hour))
	return gmtDate.Format(http.TimeFormat)
}

type Filetype int

var (
	fidSep = "|"
)

func ParseDbFid(dbFid string) (fid string, fileType Filetype, err error) {
	rs := []rune(dbFid)
	lth := len(rs)
	lastN := strings.LastIndex(dbFid, fidSep)
	if lastN < 0 {
		return string(rs[0:]), NormalFile, nil
	}
	fileTypeInt, e := strconv.Atoi(string(rs[lastN+1 : lth]))
	if e != nil {
		fileType = NormalFile
		err = nil
	} else {
		fileType = Filetype(fileTypeInt)
	}
	fid = string(rs[0:lastN])
	return fid, fileType, nil
}

func FtypeStr(f Filetype) string {
	switch f {
	case NormalFile:
		return "normal"
	case BlockedFile:
		return "blockedFile"
	case AppndFile:
		return "Appendable"
	case NormalFile_Ec:
		return "normal_ec"
	case BlockedFile_Ec:
		return "blockedFile_ec"
	default:
		return "BadFileType"
	}
}

func IsBlockFile(ftype Filetype) string {
	if ftype == BlockedFile {
		return "yes"
	} else {
		return "no"
	}
}
func IsApndFile(ftype Filetype) string {
	if ftype == AppndFile {
		return "yes"
	} else {
		return "no"
	}
}
func ParseInts(text string, defaultValue int) (int, error) {
	if text == "" {
		return defaultValue, nil
	}
	count, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func GenDbFid(fid string, ftype Filetype) (dbFid string) {
	dbFid = fid + fidSep
	switch ftype {
	case NormalFile:
		dbFid += strconv.Itoa(int(NormalFile))
	case BlockedFile:
		dbFid += strconv.Itoa(int(BlockedFile))
	case AppndFile:
		dbFid += strconv.Itoa(int(AppndFile))
	case NormalFile_Ec:
		dbFid += strconv.Itoa(int(NormalFile_Ec))
	case BlockedFile_Ec:
		dbFid += strconv.Itoa(int(BlockedFile_Ec))
	default:
		dbFid += strconv.Itoa(int(NormalFile))
	}
	return
}


func valueToString(val reflect.Value) string {
	var str string
	if !val.IsValid() {
		return "<zero Value>"
	}
	typ := val.Type()
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'g', -1, 64)
	case reflect.Complex64, reflect.Complex128:
		c := val.Complex()
		return strconv.FormatFloat(real(c), 'g', -1, 64) + "+" + strconv.FormatFloat(imag(c), 'g', -1, 64) + "i"
	case reflect.String:
		return val.String()
	case reflect.Bool:
		if val.Bool() {
			return "true"
		} else {
			return "false"
		}
	case reflect.Ptr:
		v := val
		str = typ.String() + "("
		if v.IsNil() {
			str += "0"
		} else {
			str += "&" + valueToString(v.Elem())
		}
		str += ")"
		return str
	case reflect.Array, reflect.Slice:
		v := val
		str += typ.String()
		str += "{"
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				str += ", "
			}
			str += valueToString(v.Index(i))
		}
		str += "}"
		return str
	case reflect.Map:
		t := typ
		str = t.String()
		str += "{"
		str += "<can't iterate on maps>"
		str += "}"
		return str
	case reflect.Chan:
		str = typ.String()
		return str
	case reflect.Struct:
		t := typ
		v := val
		str += t.String()
		str += "{"
		for i, n := 0, v.NumField(); i < n; i++ {
			if i > 0 {
				str += ", "
			}
			str += valueToString(v.Field(i))
		}
		str += "}"
		return str
	case reflect.Interface:
		return typ.String() + "(" + valueToString(val.Elem()) + ")"
	case reflect.Func:
		v := val
		return typ.String() + "(" + strconv.FormatUint(uint64(v.Pointer()), 10) + ")"
	default:
		panic("valueToString: can't print type " + typ.String())
	}
}


func simpleValueToString(val reflect.Value) string {
	var str string
	if !val.IsValid() {
		return "<zero Value>"
	}
	typ := val.Type()
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'g', -1, 64)
	case reflect.Complex64, reflect.Complex128:
		c := val.Complex()
		return strconv.FormatFloat(real(c), 'g', -1, 64) + "+" + strconv.FormatFloat(imag(c), 'g', -1, 64) + "i"
	case reflect.String:
		return val.String()
	case reflect.Bool:
		if val.Bool() {
			return "true"
		} else {
			return "false"
		}
	case reflect.Ptr:
		v := val
		str = typ.String() + "("
		if v.IsNil() {
			str += "0"
		} else {
			str += "&" + valueToString(v.Elem())
		}
		str += ")"
		return str
	case reflect.Array, reflect.Slice:
		v := val
		str += typ.String()
		str += "{"
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				str += ", "
			}
			str += valueToString(v.Index(i))
		}
		str += "}"
		return str
	default:
		return str

	}
}

