package operation

import ()

type JoinResult struct {
	VolumeSizeLimit uint64 `json:"VolumeSizeLimit,omitempty"`
	Error           string `json:"error,omitempty"`
}

type DescriptData struct {
        FileSize int64 `json:"FileSize,omitempty"`
        Sha1     string `json:"Sha1,omitempty"`
        Blocks   map[string]string `json:"Blocks,omitempty"`
}
