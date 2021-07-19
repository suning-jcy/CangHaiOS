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
#include <regex.h>
#include <math.h>

#define MagickPI  3.1415
#define DegreesToRadians(x) (MagickPI*(x)/180.0)

#define true 1
#define false 0
static char *COLORMH ="^#[0-9a-fA-F]{6}$";
int regexCormh(char *src)
{
     char errbuf[1024];
	 regex_t reg;
	 int err=0,nm = 10;
	 char *pattern = COLORMH;
	 regmatch_t pmatch[nm];

     if(regcomp(&reg,pattern,REG_EXTENDED) < 0){
         regerror(err,&reg,errbuf,sizeof(errbuf));
         printf("err:%s\n",errbuf);
		 return false;
     }

     err = regexec(&reg,src,nm,pmatch,0);

    if(err == REG_NOMATCH){
	   printf("str=%s \t",src);
	   printf("no match\n");
	   regfree(&reg);
       return false;
	}else if(err){
		regerror(err,&reg,errbuf,sizeof(errbuf));
		regfree(&reg);
		return false;
	}

    regfree(&reg);
	return true;
}
void setImageQuality(Image *image,ImageInfo * imageInfo){
   const ImageAttribute *qualityAttr = NULL;
   if(image==NULL||imageInfo==NULL){
        return;
   }
   qualityAttr = GetImageAttribute(image,"JPEG-Quality");
   if ( qualityAttr != NULL )
   {
     size_t imageQuality = atoi(qualityAttr->value);
     imageInfo->quality = imageQuality;
   }
   if (imageInfo->quality == 100)
   {
   	 imageInfo->quality = 99;
   }
   return;
}

void * GetImageFromText(void * data,char * primitive,int p,char* type ,char* color ,int size,int angle, int x, int y ,int *dataLen )
{
    ExceptionInfo exception;
    GetExceptionInfo(&exception);

    Image 		*image 		= NULL;
    ImageInfo  	*image_info = NULL;
	DrawInfo   	*draw_info	= NULL;
    ImageInfo  	*clone_info = NULL;
	size_t 		localDataLen=(size_t)*dataLen;
	MagickPassFail   status = MagickFail;
    image_info=CloneImageInfo((ImageInfo *) NULL);
    image = BlobToImage(image_info,data, *dataLen,&exception);
    if (!image) {
		 printf("Failed to read any images!\n");
		 CatchException(&exception);
		 data = NULL;
		 goto done;
	}
    setImageQuality(image,image_info);
    clone_info=CloneImageInfo(image_info);
    draw_info=CloneDrawInfo(clone_info,(DrawInfo *) NULL);

	if (draw_info->font == NULL) {
	    (void) CloneString(&draw_info->font,type);
	  }else {
	    (void) strncpy(draw_info->font,type,sizeof(type));
	  }
    draw_info->pointsize = size;
    draw_info->gravity = p;
	if (draw_info->text == NULL) {
	    (void) CloneString(&draw_info->text,primitive);
	  }else {
	    (void) strncpy(draw_info->text,primitive,sizeof(primitive));
	  }

    draw_info->affine.tx = x;
    draw_info->affine.ty = y;
    draw_info->affine.sx=cos(DegreesToRadians(fmod(angle,360.0)));
    draw_info->affine.rx=sin(DegreesToRadians(fmod(angle,360.0)));
    draw_info->affine.ry=(-sin(DegreesToRadians(fmod(angle,360.0))));
    draw_info->affine.sy=cos(DegreesToRadians(fmod(angle,360.0)));

	QueryColorDatabase(color,&draw_info->fill,&(image->exception));

   (void) CloneString(&draw_info->primitive,primitive);
   (void) CloneString(&draw_info->encoding,"UTF-8");

    status =  DrawImage(image,draw_info);

    if(status == MagickFail)
   	{
   	   printf("draw failed\n");
	   data = NULL;
	   goto done;
   	}

    data = ImageToBlob(image_info,image,&localDataLen,&exception);
	if (data==NULL){
		printf("failed to imagetoblob");
		CatchException(&exception);
	}
	*dataLen=localDataLen;

    done:
    if (image)
	 {
	  DestroyImageList(image);
	 }
	 if(image_info)
	 {
	    DestroyImageInfo(image_info);
	 }
	 if(draw_info)
	 {
        DestroyDrawInfo(draw_info);
	 }

	if(clone_info)
	{
		DestroyImageInfo(clone_info);
	}
     DestroyExceptionInfo(&exception);
     return data ;
}


void* GetWaterMarkFromImage(void * data, void* watermark,int *ldataLen,int *ldataLen1,int p,int s,int x,int y, int angle,int voffset,int isCompatible)
{
  Image * img = NULL;
  Image * waterImage = NULL;
  Image * newData = NULL;
  ImageInfo * image_info = NULL;
  ImageInfo * image_info1 = NULL;

  size_t dataLen1 = (size_t)*ldataLen1;
  size_t dataLen = (size_t)*ldataLen;

  MagickPassFail  status=MagickPass;

  ExceptionInfo exception;
  GetExceptionInfo(&exception);

  image_info=CloneImageInfo((ImageInfo *) NULL);
  image_info1=CloneImageInfo((ImageInfo *) NULL);

   img = BlobToImage(image_info,data,dataLen,&exception);
   if (!img) {
	  printf("Failed to read any images!\n");
	  CatchException(&exception);
      goto done;
   }
  setImageQuality(img,image_info);
  waterImage = BlobToImage(image_info1,watermark,dataLen1,&exception);
  if (!waterImage) {
	 printf("Failed to read any images!\n");
	 CatchException(&exception);
	 goto done;
  }
if(isCompatible){
		AffineMatrix affine;
		IdentityAffine(&affine);
		affine.sx = s;
		affine.sy = s;

		affine.rx = 0;
		affine.ry = 0;

		affine.tx = x;
		affine.ty = y - waterImage->rows;

		if (angle != 0 )
		{
		 affine.sx=cos(DegreesToRadians(fmod(angle,360.0)));
		 affine.rx=sin(DegreesToRadians(fmod(angle,360.0)));
		 affine.ry=(-sin(DegreesToRadians(fmod(angle,360.0))));
		 affine.sy=cos(DegreesToRadians(fmod(angle,360.0)));
		}

		waterImage->gravity = p;

		status = DrawAffineImage(img,waterImage,&affine);
	}else{
       if (angle != 0) {
       Image * rotateImage = RotateImage(waterImage,(double)angle,&exception);
       if (rotateImage == NULL ){
           printf("Failed to rotate images!\n");
	   CatchException(&exception);
	   goto done;
        }
        DestroyImageList(waterImage);
        waterImage = rotateImage;
   		}

    char  composite_geometry[MaxTextExtent];
    char  tmp[MaxTextExtent];
    RectangleInfo  geometry;
    geometry.x=0;
    geometry.y=0;
    sprintf(tmp,"+%d+%d",x,y);
    (void) GetGeometry(tmp,
                        &geometry.x,
                        &geometry.y,
                        &geometry.width,
                        &geometry.height);

    FormatString(composite_geometry,
                        "%lux%lu%+ld%+ld",
                        waterImage->columns,
                        waterImage->rows,
                        geometry.x,
                        geometry.y);
   img->gravity = p;
   (void) GetImageGeometry(img,composite_geometry,0,&geometry);
   status = CompositeImage(img, OverCompositeOp,waterImage,geometry.x,geometry.y);
   }
GetImageException(img,&exception);

   if(status != MagickPass)
   {
     printf("get watermark failed. return \n");
     CatchException(&exception);
     goto done;
    }
    newData = ImageToBlob(image_info,img,&dataLen,&exception);
    if( newData == NULL)
  	{
  	   printf("magick to blob failed.\n");
       CatchException(&exception);
	   goto done;
  	}else{
		*ldataLen=dataLen;
	}
done:

   if (img)
   {
	 DestroyImageList(img);
   }

   if (waterImage)
   {
	 DestroyImageList(waterImage);
   }

   if ( image_info)
   {
	  DestroyImageInfo(image_info);
   }
   if ( image_info1)
   {
	  DestroyImageInfo(image_info1);
   }
   DestroyExceptionInfo(&exception);
   return newData;
}
*/
import "C"
import (
	"unsafe"
)
import _ "fmt"
import _ "os"
import _ "strconv"

func GetWaterMark(data []byte, watermark []byte, p int, s int, x int, y int, r int, voffset int, isCompatible bool) (resized []byte, status int) {
	if len(data) <= 0 || len(watermark) <= 0 {
		return nil, -1
	}
	cdata := unsafe.Pointer(&data[0])
	cwatermark := unsafe.Pointer(&watermark[0])
	cs := C.int(s)
	cx := C.int(x)
	cy := C.int(y)
	cp := C.int(p)
	cr := C.int(r)
	cvoffset := C.int(voffset)
	cdatalen := C.int(len(data))
	cdatalen1 := C.int(len(watermark))
	cIsCompatible := C.int(0)
	if isCompatible {
		cIsCompatible = C.int(1)
	}

	cresizeData := C.GetWaterMarkFromImage(cdata, cwatermark, &cdatalen, &cdatalen1, cp, cs, cx, cy, cr, cvoffset, cIsCompatible)
	if cresizeData != nil {
		resized = C.GoBytes(unsafe.Pointer(cresizeData), cdatalen)
		C.free(cresizeData)
	} else {
		resized = nil
	}
	return resized, status
}

func GetTextMark(data []byte, text string, p int, fonttype string, color string, size int, x int, y int, angle int) (resized []byte) {
	cdata := unsafe.Pointer(&data[0])
	text = "text 0, 0 '" + text + "'"
	ctext := C.CString(text)
	cdatalen := C.int(len(data))
	cp := C.int(p)
	cfonttype := C.CString(fonttype)
	ccolor := C.CString(color)
	//cs := C.int(s)
	csize := C.int(size)
	cangle := C.int(angle)
	cx := C.int(x)
	cy := C.int(y)

	cresizeData := C.GetImageFromText(cdata, ctext, cp, cfonttype, ccolor, csize, cangle, cx, cy, &cdatalen)
	if cresizeData != nil {
		resized = C.GoBytes(unsafe.Pointer(cresizeData), cdatalen)
		C.free(cresizeData)
	} else {
		resized = nil
	}
	C.free(unsafe.Pointer(cfonttype))
	C.free(unsafe.Pointer(ctext))
	C.free(unsafe.Pointer(ccolor))
	return resized
}
