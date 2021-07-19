// +build gm

package images

/*
#cgo CFLAGS: -I /usr/local/include/GraphicsMagick
#cgo CFLAGS: -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector --param=ssp-buffer-size=4 -m64 -mtune=generic -Wall -pthread
#cgo LDFLAGS: -L/usr/local/lib -L/usr/lib -L/usr/lib64
#cgo LDFLAGS: -lGraphicsMagick -lwebp -ltiff -lfreetype -ljpeg -lpng12 -lz -lm -lgomp -lpthread -lbz2 -lX11 -lltdl
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <magick/api.h>
#define	MAGIC_JPEG "JPEG"
int getImageInfo(void * data, int *iLen, int *width, int *height, int *quality, char **compress)
{
 	ExceptionInfo exception;
	ImageInfo *image_info=NULL;
	Image	*image=NULL;
	size_t len=(size_t)*iLen;
	ImageAttribute *quality_attr = NULL;
	int ret=0;
	GetExceptionInfo(&exception);
   	image_info=CloneImageInfo((ImageInfo *) NULL);
	image = BlobToImage(image_info,data, len,&exception);
   	if (!image) {
		ret=1;
		goto done;
    }

	*width=image->columns;
	*height=image->rows;

	(void) CloneString(compress,image->magick);
   	if(0==LocaleCompare(image->magick,MAGIC_JPEG))
	{
		quality_attr = GetImageAttribute(image,"JPEG-Quality");
   		if(quality_attr == NULL )
   		{
			ret=2;
			goto done;
   		}
		*quality=atoi(quality_attr->value);
	}

done:
	if (image)
	{
    	DestroyImageList(image);
	}

	if ( image_info)
	{
   		DestroyImageInfo(image_info);
	}
	return ret;

}
*/
import "C"
import (
	"unsafe"
)
import _ "fmt"
import _ "os"
import _ "strconv"

func GetImageProperty(data []byte) (info *ImageInfo, ret int) {
	cdata := unsafe.Pointer(&data[0])
	clen := C.int(len(data))
	cwidth := C.int(0)
	cheight := C.int(0)
	cquality := C.int(0)
	cext := C.CString("")
	retInfo := C.getImageInfo(cdata, &clen, &cwidth, &cheight, &cquality, &cext)
	if ret = int(retInfo); ret != 0 {
		return nil, ret
	}
	info = &ImageInfo{Width: int(cwidth),
		Height:   int(cheight),
		Quality:  int(cquality),
		Format:   C.GoString(cext),
		FileSize: len(data)}
	C.free(unsafe.Pointer(cext))
	return info, ret
}
