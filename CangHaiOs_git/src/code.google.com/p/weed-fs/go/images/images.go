package images

import (
	"strings"
)

var InfoMap map[string]struct{}

type ImageInfo struct {
	FileSize int    `json:"FileSize,omitempty"`
	Format   string `json:"Format,omitempty"`
	Height   int    `json:"ImageHeight,omitempty"`
	Width    int    `json:"ImageWidth,omitempty"`
	Quality  int    `json:"ImageQuality,omitempty"`
}

func init() {
	InfoMap = make(map[string]struct{})
	var stone struct{} = struct{}{}
	InfoMap[".gif"] = stone
	InfoMap[".png"] = stone
	InfoMap[".jpg"] = stone
	InfoMap[".jpeg"] = stone
	InfoMap[".webp"] = stone
	InfoMap["image/gif"] = stone
	InfoMap["image/png"] = stone
	InfoMap["image/jpeg"] = stone
	InfoMap["image/webp"] = stone
}
func IsImage(keys ...string) bool {
	for _, key := range keys {
		if _, ok := InfoMap[strings.ToLower(key)]; ok {
			return ok
		}
	}
	return false
}
