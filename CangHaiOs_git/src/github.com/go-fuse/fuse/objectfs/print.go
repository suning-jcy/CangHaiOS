// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package objectfs

import (
	"fmt"

	"github.com/go-fuse/fuse"
)

// String provides a debug string for the given file.
func (f *openedFile) String() string {
	return fmt.Sprintf("File %s (%s) %s %s",
		f.File, f.Description, fuse.FlagString(fuse.OpenFlagNames, int64(f.OpenFlags), "O_RDONLY"),
		fuse.FlagString(fuse.FuseOpenFlagNames, int64(f.FuseFlags), ""))
}
