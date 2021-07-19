package auth

import (
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	"context"
)

type KeystoneAuth struct {
	//key:token val:lastVisit
	tokenCache    map[string]int64
	authUrl       string
	cacheInterval int
	accessLock    sync.Mutex
}

func NewKeystoneAuth(filerDir string) (ksa *KeystoneAuth, err error) {
	ksa = &KeystoneAuth{}
	am, ae := NewAuthManager(filepath.Join(filerDir, "keystone.conf"))
	if ae == nil {
		ksa.cacheInterval = am.interval
		ksa.authUrl = am.url
	} else {
		ksa.cacheInterval = 60
	}
	ksa.tokenCache = make(map[string]int64)
	go ksa.startCleanCache()
	glog.V(0).Infoln("New Auth, url:", ksa.authUrl, " interval:", ksa.cacheInterval)
	return ksa, nil
}

func (ksa *KeystoneAuth) AuthValidCluster(r *http.Request, server, clusternum string,ctx *context.Context) (access bool, err error) {
	return ksa.AuthValid(r, server,ctx)
}
func (ksa *KeystoneAuth) AuthValid(r *http.Request, server string,ctx *context.Context) (access bool, err error) {
	ksa.accessLock.Lock()
	defer ksa.accessLock.Unlock()

	var token string
	var url string
	access = false
	if authHeader, ok := r.Header["X-Auth-Token"]; ok {
		token = authHeader[0]
		glog.V(4).Infoln("valid token", token)
	} else {
		glog.V(0).Infoln("Invalid token!")
		err = errors.New("Invalid token!")
		return
	}

	if authHeader, ok := r.Header["X-Auth-Adminurl"]; ok {
		url = authHeader[0]
		glog.V(4).Infoln("valid token in url", url)
	} else {
		glog.V(0).Infoln("Invalid token in url!")
		err = errors.New("Invalid token in url!")
		return
	}
	if lastVisit, ok := ksa.tokenCache[token]; ok {
		glog.V(4).Infoln("Find in cache, last visit:", lastVisit)
		if lastVisit == int64(0) {
			glog.V(0).Infoln("token invalid ", token)
			err = errors.New("Invalid token!")
			return
		}
		if lastVisit < (time.Now().Unix() - int64(ksa.cacheInterval)) {
			glog.V(4).Infoln("Cache in invaid, valid in keystone")
			if valid, e := ksa.validTokenCache(token, url); e == nil && valid {
				ksa.updateTokenCache(token)
				access = true
			} else {
				ksa.tokenCache[token] = int64(0)
				//ksa.deleteTokenCache(token)
			}
		} else {
			glog.V(4).Infoln("Cache valid")
			access = true
		}
	} else {
		if valid, e := ksa.validTokenCache(token, url); e == nil && valid {
			glog.V(4).Infoln("new token, save in cache")
			ksa.updateTokenCache(token)
			access = true
		} else {
			ksa.tokenCache[token] = int64(0)
		}
	}
	return
}

func (ksa *KeystoneAuth) deleteTokenCache(token string) {
	ksa.accessLock.Lock()
	defer ksa.accessLock.Unlock()
	delete(ksa.tokenCache, token)
}

func (ksa *KeystoneAuth) updateTokenCache(token string) {
	ksa.tokenCache[token] = time.Now().Unix()
}

func (ksa *KeystoneAuth) validTokenCache(authToken string, authUrl string) (bool, error) {
	url := authUrl + authToken
	glog.V(4).Infoln("URL:", url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("X-Auth-Token", authToken)
	resp, err := util.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		glog.V(4).Infoln("Valid url:", url, " success")
		return true, nil
	}
	glog.V(4).Infoln("Valid url:", url, " fail")
	return false, nil
}

func (ksa *KeystoneAuth) startCleanCache() {
	c := time.Tick(1 * time.Minute)
	for _ = range c {
		ksa.accessLock.Lock()
		glog.V(4).Infoln("Clean keystone token cache...")
		for token, visit := range ksa.tokenCache {
			if visit == int64(0) {
				ksa.deleteTokenCache(token)
			}
		}
		ksa.accessLock.Unlock()
	}
}
func (ksa *KeystoneAuth) AuthType() string {
	return "KeystoneAuth"
}
