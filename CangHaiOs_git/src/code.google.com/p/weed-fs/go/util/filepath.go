package util

import (
	//"fmt"
	"net/http"
	"net/url"
	"strings"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
)

func GetImageBasePath(filePath string) (basePath string) {
	basePath = filePath
	ext := ""
	dotIndex := strings.LastIndex(filePath, ".")
	if dotIndex > 0 {
		ext = filePath[dotIndex:]
	}
	if ext == "" || !(ext == ".png" || ext == ".jpg" || ext == ".gif" || ext == ".webp") {
		return
	}
	underlineIndex := strings.LastIndex(filePath, "_")
	plusIndex := strings.LastIndex(filePath, "+")
	// 1.jpg
	// 1_10x10.jpg
	// 1+10.jpg
	// 1_10x10+10.jpg
	// 1_1.jpg
	// 1_1_10x10.jpg
	// 1_1+10.jpg
	// 1_1_10x10+10.jpg
	if underlineIndex > 0 && strings.LastIndex(filePath[underlineIndex:dotIndex], "x") > -1 {
		basePath = filePath[:underlineIndex]
	} else if plusIndex > 0 {
		basePath = filePath[:plusIndex]
	} else {
		basePath = filePath[:dotIndex]
	}
	//fmt.Println("Get Base Path:", basePath)
	return
}

func IsOrigImage(filePath string) bool {
	baseImage := GetImageBasePath(filePath)
	dotIndex := strings.LastIndex(filePath, ".")
	tmpImage := baseImage + filePath[dotIndex:]
	if tmpImage == filePath {
		return true
	}
	return false
}

func ParseServicePath(r *http.Request) (path string) {
	path, err := url.QueryUnescape(r.URL.String())
	if err != nil {
		glog.V(0).Infoln("can not ParseServicePath:", r.URL.String(), err)
		return r.URL.Path
	}
	i := strings.Index(path, "?")
	if i >= 0 {
		path = path[0:i]
	}
	return
}
func ParseServicePathCluster(r *http.Request, clusternum string) (path string) {
	path = ParseServicePath(r)
	if len(clusternum) > 0 {
		if strings.HasPrefix(path, "/"+clusternum+"/") {
			path = path[len(clusternum)+1:]
		}
	}
	return
}
func ParseServiceUrl(r *http.Request) (account string, bucket string, object string) {
	temppath := ParseServicePath(r)
	fullpath := strings.SplitN(temppath, "/", 2)
	path := fullpath[1]
	parts := strings.SplitN(path, "/", 3)
	glog.V(3).Infoln(r.URL.Path)
	switch len(parts) {
	case 1:
		return parts[0], "", ""
	case 2:
		return parts[0], parts[1], ""
	case 3:
		if parts[2] == "" && (r.Method == "PUT" || r.Method == "POST") {
			parts[2] = "/"
		}
		return parts[0], parts[1], parts[2]
	}
	return "", "", ""
}
func ParseServiceUrlForCluster(r *http.Request, clusternum string) (account string, bucket string, object string) {
	temppath := ParseServicePath(r)
	var fullpath []string
	if len(clusternum) > 0 {
		if strings.HasPrefix(temppath, "/"+clusternum+"/") {
			temppath = temppath[len(clusternum)+1:]
		}
	}
	fullpath = strings.SplitN(temppath, "/", 2)

	path := fullpath[1]
	parts := strings.SplitN(path, "/", 3)
	glog.V(3).Infoln(r.URL.Path)
	switch len(parts) {
	case 1:
		return parts[0], "", ""
	case 2:
		return parts[0], parts[1], ""
	case 3:
		if parts[2] == "" && (r.Method == "PUT" || r.Method == "POST") {
			parts[2] = "/"
		}
		return parts[0], parts[1], parts[2]
	}
	return "", "", ""
}

func ParseServiceFilepath(filepath string) (account string, bucket string, object string) {
	fullpath := strings.SplitN(filepath, "/", 2)
	path := fullpath[1]
	parts := strings.SplitN(path, "/", 3)
	switch len(parts) {
	case 1:
		return parts[0], "", ""
	case 2:
		return parts[0], parts[1], ""
	case 3:
		return parts[0], parts[1], parts[2]
	}
	return "", "", ""
}

var tags = map[string]struct{}{
	"type":  struct{}{},
	"brand": struct{}{},
}

func ParseFileTag(filetag string) string {
	filetag, err := url.QueryUnescape(filetag)
	if err != nil {
		glog.V(0).Infoln("can not ParseFileTag:", filetag, err)
		return ""
	}
	//tag太长了，超过255 ，进行截取.
	parts := strings.Split(filetag, ";")
	filetag = ""
	for _, v := range parts {
		idx := -1
		//必须是a=b形式。只看第一个“=”位置
		if idx = strings.Index(v, "="); idx <= 0 || idx == len(v)-1 {
			continue
		}
		//2-11 标签只允许 type和brand
		if _, ok := tags[v[:idx]]; !ok {
			continue
		}
		if filetag == "" {
			if len(v) > public.MAX_TAG_LENGTH {
				break
			}
			filetag = v
		} else {
			if len(filetag)+len(v)+1 > public.MAX_TAG_LENGTH {
				break
			}
			filetag += ";" + v
		}
	}
	return filetag
}

//文件名的校验
//应该是比较专业的文件名比对，现在只有个文件长度
func PathCheck(filapath string) bool {
	if len(filapath)>255{
		return  false
	}
	return true;
}
