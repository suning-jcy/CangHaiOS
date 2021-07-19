// +build gm

package images

/*
#cgo CFLAGS: -I /usr/local/include/GraphicsMagick
#cgo CFLAGS: -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector --param=ssp-buffer-size=4 -m64 -mtune=generic -Wall -pthread
#cgo LDFLAGS: -L/usr/local/lib -L/usr/lib -L/usr/lib64
#cgo LDFLAGS: -lGraphicsMagick -lwebp -ltiff -lfreetype -ljpeg -lpng12 -lz -lm -lgomp -lpthread -lbz2 -lX11 -lltdl
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <magick/api.h>

#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
int GetImageQuality(Image *image,ImageInfo * imageInfo){
	size_t  quality=0;
   const ImageAttribute *qualityAttr = NULL;
   if(image==NULL||imageInfo==NULL){
        return 0;
   }
   qualityAttr = GetImageAttribute(image,"JPEG-Quality");
   if ( qualityAttr != NULL )
   {
     size_t imageQuality = atoi(qualityAttr->value);
	 quality=imageQuality;
   }
   return quality;
}
void * GetMontageImage(void * dataList,int * len,int num,char *tile,int *dataLen )
{
	Image * image1 = NULL;
	ImageInfo * image_info1 = NULL;
	ImageInfo * image_info2 = NULL;
	size_t localDataLen=(size_t)*dataLen;
	void *data = NULL;
	void *subdata = NULL;
	size_t quality=0;
    ExceptionInfo exception;
	   GetExceptionInfo(&exception);

	Image * image = NULL;
	ImageInfo *image_info = NULL;
	image_info = CloneImageInfo((ImageInfo *) NULL);

    MontageInfo  *montage_info = NULL;
	Image* montage_image = NULL;


    image_info2=CloneImageInfo((ImageInfo *) NULL);
    int i = 0;
	int pos=0;
	for ( i = 0 ; i < num ; i++ )
	{
		image_info1=CloneImageInfo((ImageInfo *) NULL);
		if (image_info1 == NULL )
		{
                   printf("Failed to get image_info!\n");
                   CatchException(&exception);
                   goto done;
		}
		//printf("i=%d,len=%d,pos=%d,data=%d\n",i,len[i],pos,dataList+pos);
		image1 = BlobToImage(image_info1,dataList+pos, len[i],&exception);
		pos=pos+len[i];
		if (!image1)
		{
	       printf("Failed to read any images!\n");
		   goto done;
	    }
		size_t tmpQual=GetImageQuality(image1,image_info1);
		if (tmpQual>quality){
			quality=tmpQual;
		}
		if (image_info1 != NULL)
		{
		   DestroyImageInfo(image_info1);
		   image_info1 = NULL;
		}

		AppendImageToList(&image,image1);
	}

	  montage_info=CloneMontageInfo(image_info2,(MontageInfo *) NULL);

	 // montage_info->frame=(char *) NULL;
	  montage_info->shadow=0;
	 if (montage_info->geometry == NULL) {
	    (void) CloneString(&montage_info->geometry,"+0+0");
	  }else {
	    (void) strncpy(montage_info->geometry,"+0+0",sizeof("+0+0"));
	  }
	  montage_info->border_width=0;
     if ( montage_info->tile == NULL){
        (void) CloneString(&montage_info->tile,tile);
	  }else {
	    (void) strncpy(montage_info->tile,tile,sizeof(tile));
	 }
	  montage_image =MontageImages(image,montage_info,&exception);
	   if (montage_image == (Image *) NULL)
	  {
		 printf("Get Montage Image Error\n");
		goto done;
	  }

      (void) strncpy(montage_image->magick,image1->magick,sizeof(image1->magick));
	   if (quality>0){
			image_info->quality = quality;
	   }
	  data = ImageToBlob(image_info,montage_image,&localDataLen,&exception);

	  if (!data)
	  {
         printf("Failed to write images!\n");
         CatchException(&exception);
         goto done;
      }else{
		*dataLen=localDataLen;
	}

done:
    if (image_info1)
	{
		DestroyImageInfo(image_info1);
		image_info1 = NULL;
	}

    if (image_info2)
	{
		DestroyImageInfo(image_info2);
		image_info2 = NULL;
	}

    if (image_info)
	{
		DestroyImageInfo(image_info);
		image_info = NULL;
	}

    if (image)
	{
		DestroyImageList(image);
		image = NULL;
	}

    if (montage_info)
	{
	  DestroyMontageInfo(montage_info);
	  montage_info = NULL;
	}

	if (montage_image)
	{
		DestroyImageList(montage_image);
		montage_image = NULL;
	}

	return data;
}
*/
import "C"
import (
	"unsafe"
)
import _ "fmt"
import _ "os"
import _ "strconv"

func GetMontageImg(dataList [][]byte, num int, tile string) (resized []byte) {
	/*
		var buf []unsafe.Pointer
		var lenList []C.int
		for _, v := range dataList {
			if v == nil || len(v) <= 0 {
				return nil
			}
			buf = append(buf, unsafe.Pointer(&v[0]))
			lenList = append(lenList, (C.int(C.int(len(v)))))
		}
		newList := (*unsafe.Pointer)(&buf[0])
		clenList := (*C.int)(&lenList[0])
	*/
	var dataall []byte
	var lenlist []C.int
	for _, v := range dataList {
		dataall = append(dataall, v...)
		lenlist = append(lenlist, C.int(len(v)))
		//fmt.Println(len(v))
	}
	newList := unsafe.Pointer(&dataall[0])
	clenList := (*C.int)(&lenlist[0])
	cnum := C.int(num)
	ctile := C.CString(tile)

	cdatalen := C.int(0)

	cresizeData := C.GetMontageImage(newList, clenList, cnum, ctile, &cdatalen)
	if cresizeData != nil {
		resized = C.GoBytes(unsafe.Pointer(cresizeData), cdatalen)
		C.free(cresizeData)
	} else {
		resized = nil
	}
	C.free(unsafe.Pointer(ctile))
	return resized
}
