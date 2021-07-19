package util

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"context"
	"fmt"
	"code.google.com/p/weed-fs/go/public"
)

const (
	Byte     = "B"
	KiloByte = "KB"
	MegaByte = "MB"
	GigaByte = "GB"
)

func PraseToBytes(toParse string) (int64, error) {
	var i int
	var err error
	var ret int64
	var count int
	if i = strings.Index(toParse, KiloByte); i > 0 {
		count, err = strconv.Atoi(strings.Split(toParse, KiloByte)[0])
		if err == nil {
			ret = int64(count * 1024)
		}
	} else if i = strings.Index(toParse, MegaByte); i > 0 {
		count, err = strconv.Atoi(strings.Split(toParse, MegaByte)[0])
		if err == nil {
			ret = int64(count * 1024 * 1024)
		}
	} else if i = strings.Index(toParse, GigaByte); i > 0 {
		count, err = strconv.Atoi(strings.Split(toParse, GigaByte)[0])
		if err == nil {
			ret = int64(count * 1024 * 1024 * 1024)
		}
	} else if i = strings.Index(toParse, Byte); i > 0 {
		count, err = strconv.Atoi(strings.Split(toParse, Byte)[0])
		if err == nil {
			ret = int64(count)
		}
	} else {
		count = -1
		ret = -1
		err = errors.New("no unit found")
	}
	return ret, err

}

func BytesToString(toParse int) (ret string) {
	var count string
	if toParse >= 0 && toParse < 1024 { //B
		count = strconv.Itoa(toParse)
		ret = count + Byte
	} else if toParse/1024 > 0 && toParse/1024 < 1024 { //KB
		count = strconv.Itoa(toParse)
		ret = count + KiloByte
	} else if toParse/1024/1024 > 0 && toParse/1024/1024 < 1024 { //MB
		count = strconv.Itoa(toParse)
		ret = count + MegaByte
	} else if toParse/1024/1024/1024 > 0 && toParse/1024/1024/1024 < 1024 { //GB
		count = strconv.Itoa(toParse)
		ret = count + GigaByte
	} else {
		ret = ""
	}
	return
}
func RandStringArray(src []string) (dest []string) {
	l := len(src)
	if l <= 0 {
		return
	}
	rand.Seed(time.Now().Unix())
	i := rand.Intn(l)
	for j := 0; j < l; j++ {
		dest = append(dest, src[(i+j)%l])
	}
	return
}

func GetSplitTime(ctx *context.Context, splitkey string, starttime *time.Time)  {

	splittime := fmt.Sprint(splitkey, ":", (float64(time.Now().UnixNano())-float64(starttime.UnixNano()))/1000000, "ms")
	*starttime=time.Now()
	if ctx == nil {
		*ctx= context.WithValue(context.Background(), public.SDOSS_SPLIT_TIME, splittime)
	} else {
		times, _ := ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
		if times!=""{
			splittime = times + "|" + splittime
		}
		*ctx = context.WithValue((*ctx), public.SDOSS_SPLIT_TIME, splittime)
	}
	return
}

//统计某个读流程中每次EC读取分片的总耗时
func AddECReadSliceTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "ECReadSlice", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("ECReadSlice")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "ECReadSlice", fmt.Sprintf("%d",newTimes))
	}
	return
}
//统计某个读流程中EC数据不需解码时，每次数据合并的总耗时
func AddECMergeTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "ECMerge", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("ECMerge")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "ECMerge", fmt.Sprintf("%d",newTimes))
	}
	return
}
//统计某个读流程中EC数据需解码时，每次数据解码的总耗时
func AddECDecodeTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "ECDecode", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("ECDecode")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "ECDecode", fmt.Sprintf("%d",newTimes))
	}
	return
}

//统计某个写流程中EC编码时的总耗时
func AddECEncodeTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "ECEncode", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("ECEncode")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "ECEncode", fmt.Sprintf("%d",newTimes))
	}
	return
}

//统计某个写流程中上传的总耗时
func AddUploadTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "UploadData", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("UploadData")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "UploadData", fmt.Sprintf("%d",newTimes))
	}
	return
}

func AddECTime(ctx *context.Context) {
	if ctx == nil {
		return
	}
	splittime:=""
	splittime1:=""
	splittime2:=""
	splittime3:=""
	splittime4:=""
	splittime5:=""
	encodeTimes, _ := ((*ctx).Value("ECEncode")).(string)
	if encodeTimes != "" {
		*ctx = context.WithValue((*ctx), "ECEncode", "")
		splittime1 = fmt.Sprint("ECEncode", ":",encodeTimes,"ms")
	}
	uploadTimes, _ := ((*ctx).Value("UploadData")).(string)
	if uploadTimes != "" {
		*ctx = context.WithValue((*ctx), "UploadData", "")
		splittime2 = fmt.Sprint("UploadData", ":",uploadTimes,"ms")
	}
	readTimes, _ := ((*ctx).Value("ECReadSlice")).(string)
	if readTimes != "" {
		*ctx = context.WithValue((*ctx), "ECReadSlice", "")
		splittime3 = fmt.Sprint("ECReadSlice", ":",readTimes,"ms")
	}
	mergeTimes, _ := ((*ctx).Value("ECMerge")).(string)
	if mergeTimes != "" {
		*ctx = context.WithValue((*ctx), "ECMerge", "")
		splittime4 = fmt.Sprint("ECMerge", ":",mergeTimes,"ms")
	}
	decodeTimes, _ := ((*ctx).Value("ECDecode")).(string)
	if decodeTimes != "" {
		*ctx = context.WithValue((*ctx), "ECDecode", "")
		splittime5 = fmt.Sprint("ECDecode", ":",decodeTimes,"ms")
	}
	times, _ := ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	if times != ""{
		splittime = times
		if splittime1 != ""{
			splittime = splittime + "|" + splittime1
		}
	}else{
		if splittime1 != ""{
			splittime = splittime1
		}
	}
	if splittime2 != ""{
		if splittime  == ""{
			splittime = splittime2
		}else{
			splittime = splittime + "|" + splittime2
		}
	}
	if splittime3 != ""{
		if splittime  == ""{
			splittime = splittime3
		}else{
			splittime = splittime + "|" + splittime3
		}
	}
	if splittime4 != ""{
		if splittime  == ""{
			splittime = splittime4
		}else{
			splittime = splittime + "|" + splittime4
		}
	}
	if splittime5 != ""{
		if splittime  == ""{
			splittime = splittime5
		}else{
			splittime = splittime + "|" + splittime5
		}
	}
	*ctx = context.WithValue((*ctx), public.SDOSS_SPLIT_TIME, splittime)
}



//统计某个读描述文件流程中每次EC读取分片的总耗时
func AddECReadDescSliceTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "ECReadDescSlice", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("ECReadDescSlice")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "ECReadDescSlice", fmt.Sprintf("%d",newTimes))
	}
	return
}
//统计某个读描述文件流程中EC数据不需解码时，每次数据合并的总耗时
func AddECDescMergeTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "ECDescMerge", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("ECDescMerge")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "ECDescMerge", fmt.Sprintf("%d",newTimes))
	}
	return
}
//统计某个读描述文件流程中EC数据需解码时，每次数据解码的总耗时
func AddECDescDecodeTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "ECDescDecode", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("ECDescDecode")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "ECDescDecode", fmt.Sprintf("%d",newTimes))
	}
	return
}

func AddECDescTime(ctx *context.Context) {
	if ctx == nil {
		return
	}
	splittime:=""
	splittime1:=""
	splittime2:=""
	splittime3:=""
	readTimes, _ := ((*ctx).Value("ECReadDescSlice")).(string)
	if readTimes != "" {
		*ctx = context.WithValue((*ctx), "ECReadDescSlice", "")
		splittime1 = fmt.Sprint("ECReadDescSlice", ":",readTimes,"ms")
	}
	mergeTimes, _ := ((*ctx).Value("ECDescMerge")).(string)
	if mergeTimes != "" {
		*ctx = context.WithValue((*ctx), "ECDescMerge", "")
		splittime2 = fmt.Sprint("ECDescMerge", ":",mergeTimes,"ms")
	}
	decodeTimes, _ := ((*ctx).Value("ECDescDecode")).(string)
	if decodeTimes != "" {
		*ctx = context.WithValue((*ctx), "ECDescDecode", "")
		splittime3 = fmt.Sprint("ECDescDecode", ":",decodeTimes,"ms")
	}
	times, _ := ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	if times != ""{
		splittime = times
		if splittime1 != ""{
			splittime = splittime + "|" + splittime1
		}
	}else{
		if splittime1 != ""{
			splittime = splittime1
		}
	}
	if splittime2 != ""{
		if splittime == "" {
			splittime = splittime2
		}else{
			splittime = splittime + "|" + splittime2
		}
	}
	if splittime3 != ""{
		if splittime == "" {
			splittime = splittime3
		}else{
			splittime = splittime + "|" + splittime3
		}
	}
	*ctx = context.WithValue((*ctx), public.SDOSS_SPLIT_TIME, splittime)
}

//统计发送数据的耗时
func AddSendDataTime(ctx *context.Context, starttime *time.Time)  {
	timeCost := (time.Now().UnixNano()-starttime.UnixNano())/1000000
	*starttime=time.Now()
	if ctx == nil {
		*ctx = context.WithValue(context.Background(), "WriteData", fmt.Sprintf("%d",timeCost))
	} else {
		oldtimes, _ := ((*ctx).Value("WriteData")).(string)
		newTimes := timeCost
		if oldtimes != "" {
			newTimes += ParseInt64(oldtimes,0)
		}
		*ctx = context.WithValue((*ctx), "WriteData", fmt.Sprintf("%d",newTimes))
	}
	return
}

func AddSendTime(ctx *context.Context) {
	if ctx == nil {
		return
	}
	splittime:=""
	splittime1:=""
	sendTimes, _ := ((*ctx).Value("WriteData")).(string)
	if sendTimes != "" {
		*ctx = context.WithValue((*ctx), "WriteData", "")
		splittime1 = fmt.Sprint("WriteData", ":",sendTimes,"ms")
	}
	times, _ := ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	if times != ""{
		splittime = times
		if splittime1 != ""{
			splittime = splittime + "|" + splittime1
		}
	}else{
		if splittime1 != ""{
			splittime = splittime1
		}
	}
	*ctx = context.WithValue((*ctx), public.SDOSS_SPLIT_TIME, splittime)
}