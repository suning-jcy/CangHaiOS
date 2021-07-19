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
#include<signal.h>

#define UINT unsigned int
#define bool unsigned int
#define MAX_PARAM_LEN 100
#define EXT_LEN 100
#define FILENAME_LEN 100
#define MAX_FORMAT_LEN 100

#define PARAM_SEPERATOR '@'
#define FORMAT_SEPERATOR '_'
#define PIPE_SEPERATOR '|'

#define BUFF_SIZE  100
#define true 1
#define false 0
#define COLUMNS 1
#define ROWS 2
#define WEBP_MAX_SIZE 16384
#define MAXSIZE 4096

#define MagickAtoF(str) (strtod(str, (char **)NULL))
#define MagickAtoI(str) ((int) strtol(str, (char **)NULL, 10))
#define MagickAtoL(str) (strtol(str, (char **)NULL, 10))


#define MagickFreeMemory(memory) \
{ \
  void *_magick_mp=memory;      \
  MagickFree(_magick_mp);       \
  memory=0;                     \
}

#define  HTTP_PARA_ERROR 	400
#define  HTTP_NOT_FOUNT		404
#define  HTTP_LENGTH_REQUED 411

static char * exifInfo[BUFF_SIZE] =
      {
      "EXIF:GPSLatitudeRef",
      "EXIF:GPSLatitude",
      "EXIF:GPSLongitudeRef",
      "EXIF:GPSLongitude",
      "EXIF:DateTime",
      "EXIF:DateTimeOriginal",
      "EXIF:DateTimeDigitized",
      "EXIF:Make",
      "EXIF:Model",
      "EXIF:Orientation"
      };

static const char base[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
char* base64_encode(const char* data, int data_len);
char *base64_decode(const char* data, int data_len);


enum ERRNO
{
  FORMAT_ERROR = 1,
};

typedef struct _param{
  UINT enable;
  unsigned long width;
  unsigned long height;
  UINT large;
  UINT edge;
  bool crop;
  UINT percent;
  char adcrop[MAX_PARAM_LEN];
  char rccrop[MAX_PARAM_LEN];
  char rc[MAX_PARAM_LEN];
  char circle[MAX_PARAM_LEN];
  char bgc[MAX_PARAM_LEN];
  UINT quality;
  UINT abquality;
  char src_ext[EXT_LEN];
  char des_ext[EXT_LEN];
  char src_filename[FILENAME_LEN];//include ext name
  char des_filename[FILENAME_LEN];
  UINT rotate;
  UINT orientation;
  UINT progress;
  UINT sharp;
  char blur[MAX_PARAM_LEN];
  int brightness;
  int contrast;
  UINT exif;
  char ic[MAX_PARAM_LEN];
  char wartermark[1000];
  UINT strip;
  UINT white;
  UINT format;
  UINT whflag;   //format only has h or w parameter
  UINT ff;
  UINT scaleflag;
  UINT lvlflag;
}param;

static char *REG_PATTERN = "^[0-9]*-[0-9]*bl$|"
                    "^[0-9]*-[0-9]*-[0-9]*-[0-9]*a$|"
                    "^[0-9]*[x|X][0-9]*-[0-9]rc$|"
                    "^[0-9]*[x|X]-[0-9]rc$|"
                    "^[x|X][0-9]*-[0-9]rc$|"
                    "^[0-9]*[x|y]-[0-9]*ic$|"
                    "^[0-9]*-[0-9]*-[0-9]*bgc$|"
                    "^-[0-9]*d$|"
                    "^-[0-9]*b$|"
                    "^[0-9]*d$|"
                    "^[0-9]*sh$|"
                    "^[0-9]*wh$|"
                    "^[0-9]*pr$|"
					"^[0-1]?rp$|"
					"^is$|"
                                        "^[1-3]le|"
					"^ff$|"
                    "^[0-9]*[w|h|e|c|p|q|Q|r|o|b|l]$";
static char *WIDTHHEIGTH = "^[0-9]*h$|"
                    "^[0-9]*w$|"
					"^[0-9]*p$|"
                    "^[0-9]*w_[0-9]*h$|"
					"^[0-9]*h_[0-9]*w$|"
					"^[0-9]*w_[0-9]*p$|"
					"^[0-9]*p_[0-9]*w$|"
					"^[0-9]*h_[0-9]*p$|"
					"^[0-9]*p_[0-9]*h$|"
					"^[0-9]*h_[0-9]*w_[0-9]*p$|"
					"^[0-9]*h_[0-9]*p_[0-9]*w$|"
					"^[0-9]*w_[0-9]*h_[0-9]*p$|"
					"^[0-9]*w_[0-9]*p_[0-9]*h$|"
					"^[0-9]*p_[0-9]*w_[0-9]*h$|"
                    "^[0-9]*p_[0-9]*h_[0-9]*w$";
static char *BGCMH = "^(25[0-5]|2[0-4][0-9]|(1[0-9]|[1-9])?[0-9])-(25[0-5]|2[0-4][0-9]|(1[0-9]|[1-9])?[0-9])-(25[0-5]|2[0-4][0-9]|(1[0-9]|[1-9])?[0-9])$";
static char *EXTMH ="^(jpg|png|webp|bmp)$";
int regexMatch(char *src);
int regexWidthHeigthMatch(char *src);
int regexBgcmh(char *src);
int regexExtmh(char *src);
void * cmogrifyimage(char * src_ext,char ** des_ext, void * data ,int* dataLen,int width,int height,int quality,int strip,int filler,int isLarge,int extent );

int validate(char * s);
int getParam(char *str,param* out);
int getParams(char *str, param* out);
int getParamFromInput(char * str, char* str1,param * out1);
void * getImgData(int* dataLen, void * data, char * format, char* srcext,int maxsize, int *status,char **outext,int *scaleflag,int *lvlflag,int cframelimt,int cgifproduct);
void * getImgDataEx(int * dataLen, void  * data, char * format,char *srcext,char **outext, param* param,size_t* length,int maxColRowSize,int *status,ExceptionInfo *exception,int cframelimt,int cgifproduct);
Image* getOneImage(Image* image, param *pa,int maxColRowSize,ExceptionInfo *exception);

void  GetExifInfo(Image* image,int baseinfo)
{
  char info[10240] = {0};
  const ImageAttribute
        *GPSLatitudeRef,*GPSLatitude,*GPSLongitudeRef,*GPSLongitude,*DateTime,*DateTimeOriginal,*DateTimeDigitized,*Make,*Model,*Orientation;

  GPSLatitudeRef = GetImageAttribute(image,"EXIF:GPSLatitudeRef");
  GPSLatitude = GetImageAttribute(image,"EXIF:GPSLatitude");
  GPSLongitudeRef = GetImageAttribute(image,"EXIF:GPSLongitudeRef");
  GPSLongitude = GetImageAttribute(image,"EXIF:GPSLongitude");
  DateTime = GetImageAttribute(image,"EXIF:DateTime");
  DateTimeOriginal = GetImageAttribute(image,"EXIF:DateTimeOriginal");
  DateTimeDigitized = GetImageAttribute(image,"EXIF:DateTimeDigitized");
  Make = GetImageAttribute(image,"EXIF:Make");
  Model = GetImageAttribute(image,"EXIF:Model");
  Orientation = GetImageAttribute(image,"EXIF:Orientation");

  if(GPSLatitudeRef == NULL && baseinfo == 0)
  {
     printf("<Error>\n"
		  "<Code>BadRequest</Code>\n"
		  "<Message>Image has no exif info.</Message>\n"
		  "<RequestId>5502D98553F47BFAB7F95B8C</RequestId>\n"
		  "<HostId>image-demo.img.aliyuncs.com</HostId>\n"
		  "</Error>\n");
	 return;
  }
  if(GPSLatitudeRef == NULL && baseinfo == 1)
  {
      UINT FileSize = GetBlobSize(image);
      sprintf(info,"{\nFileSize: {value: %d\n},\n" ,FileSize);
      sprintf(info + strlen(info),"{\nFormat: {value: %s\n},\n", image->magick);
      sprintf(info + strlen(info),"{\nImageHeight: {value: %ld\n},\n", image->columns);
      sprintf(info + strlen(info),"{\nImageWidth: {value: %ld\n},\n", image->rows);
	  printf(info);
	  return;
  }

  if(baseinfo == 0)
  {
	  sprintf(info,"{\nGPSLatitudeRef: {value: %s\n},\n" ,GPSLatitudeRef->value);
	  sprintf(info + strlen(info),"{\nGPSLatitude: {value: %s\n},\n", GPSLatitude->value);
	  sprintf(info + strlen(info),"{\nGPSLongitudeRef: {value: %s\n},\n", GPSLongitudeRef->value);
	  sprintf(info + strlen(info),"{\nGPSLongitude: {value: %s\n},\n" ,GPSLongitude->value);
	  sprintf(info + strlen(info),"{\nDateTime: {value: %s\n},\n" ,DateTime->value);
	  sprintf(info + strlen(info),"{\nDateTimeOriginal: {value: %s\n},\n", DateTimeOriginal->value);
	  sprintf(info + strlen(info),"{\nDateTimeDigitized: {value: %s\n},\n", DateTimeDigitized->value);
	  sprintf(info + strlen(info),"{\nMake: {value: %s\n},\n" ,Make->value);
	  sprintf(info + strlen(info),"{\nModel: {value: %s\n},\n", Model->value);
	  sprintf(info + strlen(info),"{\nOrientation: {value: %s\n},\n", Orientation->value);
  }

  if(baseinfo == 1)
  {
      magick_off_t FileSize = GetBlobSize(image);
      sprintf(info,"{\nFileSize: {value: %ld\n},\n" ,FileSize);
      sprintf(info + strlen(info),"{\nFormat: {value: %s\n},\n", image->magick);
      sprintf(info + strlen(info),"{\nImageHeight: {value: %ld\n},\n", image->columns);
      sprintf(info + strlen(info),"{\nImageWidth: {value: %ld\n},\n", image->rows);
	  sprintf(info + strlen(info),"{\nGPSLatitudeRef: {value: %s\n},\n" ,GPSLatitudeRef->value);
	  sprintf(info + strlen(info),"{\nGPSLatitude: {value: %s\n},\n", GPSLatitude->value);
	  sprintf(info + strlen(info),"{\nGPSLongitudeRef: {value: %s\n},\n", GPSLongitudeRef->value);
	  sprintf(info + strlen(info),"{\nGPSLongitude: {value: %s\n},\n" ,GPSLongitude->value);
	  sprintf(info + strlen(info),"{\nOrientation: {value: %s\n},\n", Orientation->value);
  }

  printf(info);


}

Image * GetExtentImage(Image* image, unsigned long width, unsigned long height,ExceptionInfo *exception)
{
    if(image == NULL)
		return NULL;

    char geoBuf[40] = {0};
	RectangleInfo  geometry;
	Image* extent_image = NULL;
	if ((width > 0 || height > 0))
	{
	     if (width <= 0)
		 {
			sprintf(geoBuf,"x%ld",height);
		 }
		 else if (height <=0)
		 {
			sprintf(geoBuf,"%ldx",width);
		 }
		 else
		 {
		    sprintf(geoBuf,"%ldx%ld",width,height);
	     }
	     (void) GetImageGeometry(image,geoBuf,MagickFalse,&geometry);
 		 if (geometry.width == 0)
 		    geometry.width=image->columns;
 		 if (geometry.height == 0)
 		    geometry.height=image->rows;

         geometry.x+=(long) (image->columns/2 - geometry.width/2);
         geometry.y =(long) (image->rows/2-geometry.height/2);

 		 geometry.x=(-geometry.x);
 		 geometry.y=(-geometry.y);
		 extent_image =  ExtentImage(image,&geometry,exception);
	  }
	return extent_image;
}

Image * GetScaleImage(Image* image, unsigned long width, unsigned long height,int isLarge,int edge,int maxColRowSize,ExceptionInfo *exception)
{
     if (image == NULL)
	 	 return NULL;

	 Image* scale_image = NULL;
	 RectangleInfo geometry;
	 memset(&geometry,0,sizeof(RectangleInfo));
     char geoBuf[40] = {0};
	 unsigned int size_to_fit = MagickTrue;



	 if(width > 0 || height > 0)
	 {
			if (width <= 0)
			{
			     if(!isLarge)
				     sprintf(geoBuf,"x%ld",height);
				 else
				 	 sprintf(geoBuf,"x%ld>",height);
			}
			else if (height <=0)
			{
			     if (!isLarge)
				    sprintf(geoBuf,"%ldx",width);
				 else
				 	sprintf(geoBuf,"%ldx>",width);
			}
			else
			{
			 if (edge == 0)
			 {
				  if (!isLarge)
				  {
					 sprintf(geoBuf,"%ldx%ld",width,height);
				  }
				  else
				  {
                     sprintf(geoBuf,"%ldx%ld>",width,height);
				  }
			 }
			 else if  (edge == 1 )
			 {
			   if (!isLarge)
			  {
				 sprintf(geoBuf,"%ldx%ld^",width,height);
    		  }
			  else
			  {
                  sprintf(geoBuf,"%ldx%ld^>",width,height);
			   }
			 }
			 else if(edge == 2)
			 {
			    size_to_fit = MagickFalse;

                if(!isLarge)
				     sprintf(geoBuf,"%ldx%ld",width,height);
				else
				   	 sprintf(geoBuf,"%ldx%ld>",width,height);
			 }
			 else if (edge == 4)
			 {
			    if(!isLarge)
    			sprintf(geoBuf,"%ldx%ld",width,height);
				else
				sprintf(geoBuf,"%ldx%ld>",width,height);
			 }

			}

			(void) GetImageGeometry(image,geoBuf,size_to_fit,&geometry);

			if(geometry.width > maxColRowSize || geometry.height > maxColRowSize)
			{
				return image;
			}

	 }

	scale_image = ScaleImage(image,geometry.width,geometry.height,exception);
	//scale_image = ThumbnailImage(image,geometry.width,geometry.height,exception);
	return scale_image;
}

void * getImgData(int * dataLen, void * data, char * format,char * srcext,int maxColRowSize,int *status,char **outext,int *scaleflag,int *lvlflag,int cframelimt,int cgifproduct)
{
  ExceptionInfo exception;
  size_t length = 0;
  //int result = 0;
  GetExceptionInfo(&exception);

  if(strlen(format) >= MAX_FORMAT_LEN)
  {
	  *status = HTTP_PARA_ERROR;
	  return NULL;
  }

  char *p = strchr(format,'|');
  char *tmp = NULL;
  char format_buff[BUFF_SIZE] = {0};
  unsigned char *old_data = NULL;
  int  i=0;

  //InitializeMagick(NULL);
  unsigned char
  	* new_data;

  param param,param1;
  memset(&param,0,sizeof(param));
  param.large=1;
  memset(&param1,0,sizeof(param1));
  param1.large=1;
  new_data = data;
  tmp = format;
  strncpy(format_buff,tmp ,strlen(tmp));
  while((p = strchr(tmp,'|')))
  {  memset(&param,0,sizeof(param));
     memset(format_buff,0,sizeof(format_buff));
     strncpy(format_buff,tmp ,p - tmp);
	 getParamFromInput(srcext,format_buff,&param);
	if (param.scaleflag!=0)
	 {
	    *scaleflag=param.scaleflag;
	 }
         *lvlflag = param.lvlflag;
	 if(new_data != NULL)
	 {
		old_data = new_data;
	    new_data = getImgDataEx(dataLen,old_data,format_buff,srcext,outext,&param,&length,maxColRowSize,status,&exception,cframelimt,cgifproduct);
	 	if( old_data != NULL && i!=0 && new_data!=old_data&&old_data!=data )
		{
			free(old_data);
			old_data=NULL;
		}
	}
	i++;
	 tmp = p + 1;
  }

  if(new_data != NULL )
  {
	  memset(&param1,0,sizeof(param1));
	  param1.large=1;
	  memset(format_buff,0,sizeof(format_buff));
	  strncpy(format_buff,tmp ,strlen(tmp));
	  if(param.des_ext[0] != '\0')
	  {
	      strncpy(param1.des_ext,param.des_ext,sizeof(param.des_ext));
	      getParamFromInput(param.des_ext,format_buff,&param1);
	  }
	  else
	  {
		  getParamFromInput(srcext,format_buff,&param1);
	  }
	  if (param1.scaleflag!=0)
	 {
	    *scaleflag=param1.scaleflag;
	 }
    *lvlflag = param1.lvlflag;
	  ExceptionInfo exception1;
	  GetExceptionInfo(&exception1);
	  old_data = new_data;
	  new_data = getImgDataEx(dataLen,old_data,format_buff,srcext,outext,&param1,&length,maxColRowSize,status,&exception1,cframelimt,cgifproduct);
	  if(old_data !=NULL && old_data != data && new_data!=old_data )
	  {
		free(old_data);
		old_data=NULL;
	  }
	  DestroyExceptionInfo(&exception1);
  }

  DestroyExceptionInfo(&exception);
  return new_data;


}

void * getImgDataEx(int * dataLen, void  * data, char * format, char *srcext,char **outext,param* pa,size_t* length,int maxColRowSize,int * status,ExceptionInfo *exception,int cframelimt,int cgifproduct)
{
  Image *images = NULL,*image = NULL, *new_images = NULL, *resize_image = NULL,*co_images=NULL;
  Image *  montage_image = NULL;
  ImageInfo *image_info = NULL;
  size_t ldataLen=0;
  MontageInfo  *montage_info = NULL;
  ImageInfo *image_info2 = NULL;

  int quality = 0 ;
  int abquality = 0;

  char *ext = NULL;
  int width = 0;
  int height = 0;
  int cou =0;
  int multiflag=0;
  char *outdata=NULL;
  int shrinkflag=0;

  new_images = NewImageList();

  ext = pa->des_ext;

  if(pa->quality != 0)
  	quality = pa->quality;
  if(pa->abquality != 0);
     abquality = pa->abquality;
  	image_info=CloneImageInfo((ImageInfo *) NULL);
    if (image_info==NULL)
	{
		*status = HTTP_LENGTH_REQUED ;
		data = NULL;
		goto done;
	}
   if(pa->progress != 0)
   {
	 image_info->interlace = LineInterlace;
   }

  if(pa->width != 0 && pa->width > maxColRowSize )
	{pa->enable = 0;}
  if(pa->height != 0 && pa->height > maxColRowSize )
	{pa->enable = 0;}

   if(pa->enable == 0 )
   {
		*status = HTTP_PARA_ERROR;
		data = NULL;
		goto done;
   }
     if (pa->ff)
	{
		image_info->subrange=1;
	}
    if(!strcmp(srcext,"pdf")){                     //pdf to image
        cframelimt=4;                              //pdf pages <= 4
  	    (void) CloneString(&image_info->density,"150");
		image_info->quality=pa->quality;
   }
   images = BlobToImage(image_info,data,*dataLen,exception);
   if (!images) {
	  *status = HTTP_LENGTH_REQUED;
	  CatchException(exception);
      data = NULL;
      goto done;
   }

   if (images->next != (Image *) NULL)  //gif  and  pdf
   {
		long lFrameCnt = 0L;
		size_t lFirstFrameLength = 0;

		Image *pImageHead = images;

		while(pImageHead){
		    lFrameCnt ++;
		    pImageHead = pImageHead->next;
		}
        long lGifSize = lFrameCnt*(*dataLen)/1024;
        if(lFrameCnt > cframelimt ){
            ldataLen=*dataLen;
            goto done;
		}else if(!strcmp(srcext,"pdf")){   //pdf
			//null
		}else if( lGifSize > cgifproduct ){
            ldataLen=*dataLen;
            goto done;
        }else{
	  	    co_images=CoalesceImages(images,exception);
		    if(co_images != (Image *) NULL)
	            {
	                DestroyImageList(images);
	                images=co_images;
	            }
		}
   }

   const ImageAttribute *quality_attr = NULL;

   quality_attr = GetImageAttribute(images,"JPEG-Quality");
   if ( quality_attr != NULL )
   {
     size_t image_quality = atoi(quality_attr->value);
     image_info->quality = image_quality;
   }

   if(abquality)
   {
	  if(abquality < image_info->quality)
	  {
         image_info->quality = abquality;
	  }
   }
   else if(quality)
   	  image_info->quality = image_info->quality * quality / 100;
    width = images->columns;
	height = images->rows;
	if(pa->percent != 0)
	{
		if(pa->percent < 100)
		{
			shrinkflag=1;
		}
	}
	else
	{
		if(width>=pa->width && height>=pa->height)
		{
			shrinkflag=1;
		}
	}
    if(strcmp(ext,"webp")== 0)
	{
		if( width >= WEBP_MAX_SIZE || height >= WEBP_MAX_SIZE )
		{
			strcpy(ext,pa->src_ext);
		}
	}

    while ((image=RemoveFirstImageFromList(&images)) != (Image *) NULL)
   	{
	  cou++;
      if(image->columns < width || image->rows < height)
	  {
		   ldataLen = *dataLen;
		   DestroyImage(image);
		   goto done;
	  }
   	  resize_image = getOneImage(image, pa,maxColRowSize,exception);
      if (resize_image == (Image *) NULL)
        {
          CatchException(exception);
          continue;
        }
		else
		{
	      AppendImageToList(&new_images,resize_image);
		}
   	}

  if(new_images != NULL)
  {
    if(!strcmp(srcext,"pdf") && cou > 1){        //More than one page of pdf,montage
        char tile[100]= "1x";
		char sCou[100] = {0};
		sprintf(sCou,"%ld",cou);
		strcat(tile,sCou);
        montage_info = NULL;
        montage_info=CloneMontageInfo(image_info,(MontageInfo *) NULL);
		if(*outext!=NULL){
			(void) CloneString(outext,new_images->magick);
		}
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
         montage_image =MontageImages(new_images,montage_info,exception);
         if (montage_image == (Image *) NULL)
          {
				ldataLen=0;
				data=NULL;
                goto done;
          }
          (void) strncpy(montage_image->magick,ext,sizeof(montage_image->magick));
          outdata = ImageToBlob(image_info,montage_image,&ldataLen,exception);
  		  data=outdata;
	}else{
		(void) strncpy(new_images->magick,ext,sizeof(new_images->magick));
		(void) strncpy(new_images->filename,"",sizeof(new_images->filename));
		if(*outext!=NULL){
			(void) CloneString(outext,new_images->magick);
		}

   	 	outdata = ImageToBlob(image_info,new_images,&ldataLen,exception);
		if(pa->whflag==1 && ldataLen>*dataLen && shrinkflag==1 && cou>1)
		{
			free(outdata);
			ldataLen=*dataLen;
		}
		else
		{
			data=outdata;
		}
	}
  }
  else
  {
	data = NULL;
	goto done;
  }

  if (!data) {
       printf("Failed to write images!\n");
       CatchException(exception);
       goto done;
  }
done:
  if (new_images)
  {
    DestroyImageList(new_images);
  }

  if(montage_info){
	  DestroyMontageInfo(montage_info);
	  montage_info = NULL;
  }
  if(montage_image){
	  DestroyImageList(montage_image);
      montage_image=NULL;
  }

  if (images)
  {
    DestroyImageList(images);
  }

  if ( image_info)
  {
     DestroyImageInfo(image_info);
  }
  if ( image_info2)
  {
     DestroyImageInfo(image_info2);
  }

  *dataLen = ldataLen;
  return data;
}


Image * getOneImage(Image* image, param* pa, int maxColRowSize,ExceptionInfo *exception)
{
  Image *new_image = image;
  RectangleInfo geometry;

  unsigned long width = 0;
  unsigned long height = 0;
  int quality = 0 ;int strip = 0; int isLarge = 0;
  int crop = 0;int edge = 0;int abquality = 0;int gravity = -1;

  int x=0,y=0;
  int adcrop = 0;
  int scale = 1;
  int extent = 0;

  if(pa->enable == 0)
      return image;

  if(pa->width != 0)
  	width = pa->width;
  if(pa->height != 0)
  	height = pa->height;
  if(pa->strip != 0)
  	strip = pa->strip;
  if(pa->large != 0)
  	isLarge = 1;
  if(pa->crop != 0 )
  	crop = 1;
  if (strcmp(pa->des_ext,".jpg")==0 ||strcmp(pa->des_ext,".jpeg")==0 )
   {
	if(pa->quality != 0)
  	quality = pa->quality;
  	if(pa->abquality != 0);
    abquality = pa->abquality;
   }


  edge = pa->edge;

  if(edge == 4)
  {
     extent = 1;
  }

  if(pa->format == 1)
  {
     crop = extent = scale = 0;
  }

   if(width > maxColRowSize || height > maxColRowSize)
   {
      return image;
   }

   if(pa->exif == 1)
   {
	  GetExifInfo(image,0);
      return image;
   }

   if(pa->exif == 2)
   {
	  GetExifInfo(image,1);
      return image;
   }
   if (pa->strip){
    (void) StripImage(image);
   }
   if(pa->white!= 0)
   	{
      new_image =  FlattenImages(image,exception);
	 if(new_image == (Image*) NULL)
	 {
    	CatchException(exception);
    	return image;
     }
	 else
	 {
	  DestroyImage(image);
      image=new_image;
	 }
   	}

   if(pa->adcrop[0] != '\0')
   {
	  scale = 0;
      sscanf(pa->adcrop,"%d-%d-%ld-%ld",&x,&y,&width,&height);

      geometry.width = width;
	  geometry.height = height;
	  geometry.x = x;
	  geometry.y = y;

	  new_image = CropImage(image,&geometry,exception);

	 if(new_image == (Image*) NULL)
	 {
    	CatchException(exception);
    	return image;
     }
	 else
	 {
	  new_image->page.x=0;
	  new_image->page.y=0;
	  DestroyImage(image);
      image=new_image;
	 }
   }

   if(pa->rc[0] != '\0' )
   {

       if(pa->rc[0] == 'x')
       	{
	       sscanf(pa->rc,"x%lu-%d",&geometry.height,&gravity);
		   geometry.width = image->columns;
       	}
	    else if(strstr(pa->rc,"x-") != NULL )
		{
		   sscanf(pa->rc,"%lux-%d",&geometry.width,&gravity);
		   geometry.height = image->rows;
		}
		else
		{
	       sscanf(pa->rc,"%lux%lu-%d",&geometry.width,&geometry.height,&gravity);
		}

		if(geometry.width == 0)
		{
			geometry.width = image->columns;
		}

		if(geometry.height == 0)
		{
			geometry.height = image->rows;
		}

	   image->gravity = gravity;
	   geometry.x = geometry.y = 0;
	   crop = 0;
	   scale = 0;


	  switch (image->gravity)
	  {
	    case ForgetGravity:
	    case NorthWestGravity:
	      break;
	    case NorthGravity:
	    {
	      geometry.x +=(long) (image->columns/2 - geometry.width/2 );
	      break;
	    }
	    case NorthEastGravity:
	    {
	      geometry.x=(long) (image->columns-geometry.width-geometry.x);
	      break;
	    }
	    case WestGravity:
	    {
	      geometry.y+=(long) (image->rows/2-geometry.height/2);
	      break;
	    }
	    case StaticGravity:
	    case CenterGravity:
	    default:
	    {
	      geometry.x +=(long) (image->columns/2-geometry.width/2);
	      geometry.y +=(long) (image->rows/2-geometry.height/2);
	      break;
	    }
	    case EastGravity:
	    {
	      geometry.x=(long) (image->columns-geometry.width-geometry.x);
	      geometry.y+=(long) (image->rows/2-geometry.height/2);
	      break;
	    }
	    case SouthWestGravity:
	    {
	      geometry.y=(long) (image->rows-geometry.height-geometry.y);
	      break;
	    }
	    case SouthGravity:
	    {
	      geometry.x+=(long) (image->columns/2-geometry.width/2);
	      geometry.y=(long) (image->rows-geometry.height-geometry.y);
	      break;
	    }
	    case SouthEastGravity:
	    {
	      geometry.x=(long) (image->columns-geometry.width-geometry.x);
	      geometry.y=(long) (image->rows-geometry.height-geometry.y);
	      break;
	    }
	  }

	 new_image = CropImage(image,&geometry,exception);

	 if(new_image == (Image*) NULL)
	 {
    	CatchException(exception);
    	return image;
     }
	 else
	 {
	  	new_image->page.x=0;
	        new_image->page.y=0;
	  DestroyImage(image);
      image=new_image;
	 }


	if((width > 0 || height > 0))
   	{
		new_image = GetScaleImage(image,width,height,isLarge,edge,maxColRowSize,exception);
		 if(new_image == (Image*) NULL)
		 {
	    	CatchException(exception);
	    	return image;
	     }
		 else if(new_image!=image)
		 {
		  DestroyImage(image);
	      image=new_image;
		 }
   	}

   }

	if(pa->orientation != 0)
	{
	   if(pa->orientation == 3)
	   {
		 if(image != (Image*) NULL)
		 {StripImage(image);}
	   }

	   if(pa->orientation == 2)
	   	{
		   scale = 1;
	   	   const ImageAttribute  *attribute;
           int orientation;
		   attribute = GetImageAttribute(image,"EXIF:Orientation");
		   if(attribute == NULL)
		   	{
				printf("No exif info\n");
				CatchException(exception);
		   	}else
			{
			   orientation=MagickAtoI(attribute->value);
			     new_image = AutoOrientImage(image,orientation,exception);
				 if(new_image == (Image*) NULL)
				 {
			    	CatchException(exception);
			    	return image;
			     }
				 else
				 {
			      StripImage(new_image);
				  new_image->orientation = UndefinedOrientation;
				  DestroyImage(image);
			      image=new_image;
				 }
		    }
	   	}
	}

    if(pa->rotate != 0)
    {
       new_image = RotateImage(image,(double)pa->rotate,exception);

		 if(new_image == (Image*) NULL)
		 {
	    	CatchException(exception);
	    	return image;
	     }
		 else
		 {
	     	new_image->page.x=0;
	  		new_image->page.y=0;
		  DestroyImage(image);
	      image=new_image;
		 }
    }

	if(pa->sharp != 0)
   	{
	  //printf("sh_ra=%d sh=%f",pa->sharp_ra,(double)pa->sharp);
	  int radius=0;
	  radius=(pa->sharp+99)/100;
      new_image = SharpenImage(image,(double)radius,(double)pa->sharp/100.0,exception);
	  if(new_image == (Image*) NULL)
	  {
	    	CatchException(exception);
	    	return image;
	  }
	  else
	  {
		  DestroyImage(image);
	      image=new_image;
	  }
   	}


   if(pa->blur[0]!= '\0')
   {
     int radius , sigma;
	 sscanf(pa->blur,"%d-%d",&radius,&sigma);
	 new_image = BlurImage(image,(double)radius,(double)sigma,exception);
	 if(new_image == (Image*) NULL)
	 {
    	CatchException(exception);
    	return image;
     }
	 else
	 {
	  DestroyImage(image);
      image=new_image;
	 }
   }

   if(pa->brightness !=0)
   	{

   	  char buff[BUFF_SIZE] = {0};
	  FormatString(buff,"%d,%d,%d",100 + pa->brightness,100,100);
	  if(MagickFail == ModulateImage(image,buff))
	  {
	     printf("Modulate failed.\n");
		 CatchException(exception);
		 return image;
	  }
   	}

    if(pa->contrast != 0)
	{
	  int c = 0;
	  if( pa->contrast > 0 )
	  	c = 1;
	  if (pa->contrast < 0 )
	  	c = 0;
	  if(MagickFail == ContrastImage(image,c))
	  {
	     printf("Contrast failed.\n");
		 CatchException(exception);
		 return image;
	  }
	}

	if(pa->ic[0] != '\0')
	{
	    scale = 0;
	    int xlen = 0;
		int ylen = 0;
		int index = 0;
		int max_index = 0;
		char *p1 = strchr(pa->ic,'x');
		char *p2 = strchr(pa->ic,'y');
	    if(p1)
	    {
	       sscanf(pa->ic,"%dx-%d",&xlen,&index);
	    }
	    if(p2)
	    {
			sscanf(pa->ic,"%dy-%d",&ylen,&index);
	    }
		if(xlen)
		{
		    max_index = image->columns / xlen ;
		}
		if(ylen)
		{
		    max_index = image->rows / ylen ;
		}

		if( index > max_index)
		{
		}
		else
		{
		  if(xlen)
		  {
		     x = index * xlen;
			 width = xlen;
			 height = image->rows;
		  }
		  if(ylen)
		  {
		     y = index * ylen;
			 height = ylen;
			 width= image->columns;
		  }
		}

		geometry.x = x;
		geometry.y = y;
		geometry.width = width;
		geometry.height = height;

	     new_image = CropImage(image,&geometry,exception);

		 if(new_image == (Image*) NULL)
		 {
	    	CatchException(exception);
	    	return image;
	     }
		 else
		 {
			new_image->page.x=0;
	        new_image->page.y=0;
		  DestroyImage(image);
	      image=new_image;
		 }
	}


	if(pa->percent != 0)
	{
	   if(pa->percent > 1000)
	   {
	      pa->percent = 100;
	   }
	   width  = round((double)(image->columns * pa->percent / 100));
	   height = round((double)(image->rows    * pa->percent / 100));


	   if(width > maxColRowSize || height > maxColRowSize)
	   {
          return image;
	   }
	   new_image = ZoomImage(image,width,height,exception);
		 if(new_image == (Image*) NULL)
		 {
	    	CatchException(exception);
	    	return image;
	     }
		 else
		 {
		  DestroyImage(image);
	      image=new_image;
		 }
	   scale = 0;
	}

   if(pa->bgc[0] != '\0')
   	{
   	   unsigned short a,b,c;
	   sscanf(pa->bgc,"%hu-%hu-%hu",&a,&b,&c);
	   image->background_color.red = a;
	   image->background_color.green = b;
	   image->background_color.blue = c;
   	}

   if(!adcrop)
   	{

        if(scale && (width > 0 || height > 0))
        {
		   	if((width > 0 || height > 0) && (image->columns != width || image->rows != height))
		   	{
				new_image = GetScaleImage(image,width,height,isLarge,edge,maxColRowSize,exception);
				if(new_image == (Image*) NULL)
				 {
			    	CatchException(exception);
			    	return image;
			     }
				 else if(new_image!=image)
				 {
				  	DestroyImage(image);
			      	image=new_image;
				 }
		   	}
        }

	   if(crop)
	   	{
     		geometry.width = width;
			geometry.height = height;

			geometry.x = 0;
			geometry.y = 0;
			image->gravity = CenterGravity;
	        geometry.x +=(long) (image->columns/2-geometry.width/2);
	        geometry.y +=(long) (image->rows/2-geometry.height/2);

		    new_image = CropImage(image,&geometry,exception);

			 if(new_image == (Image*) NULL)
			 {
		    	CatchException(exception);
		    	return image;
		     }
			 else
			 {
				new_image->page.x=0;
	        new_image->page.y=0;
			  DestroyImage(image);
		      image=new_image;
			 }
	   	}
   	}


   if( pa->orientation == 1)
   {
	   const ImageAttribute  *attribute;
	   int orientation;
	   attribute = GetImageAttribute(image,"EXIF:Orientation");
	   if(attribute == NULL)
	   	{
			printf("No exif info\n");
			CatchException(exception);
	   	}else
		{
		     orientation=MagickAtoI(attribute->value);
		     new_image = AutoOrientImage(image,orientation,exception);
			 if(new_image == (Image*) NULL)
			 {
		    	CatchException(exception);
		    	return image;
		     }
			 else
			 {
			  StripImage(new_image);
			  new_image->orientation = UndefinedOrientation;
			  DestroyImage(image);
		      image=new_image;
			 }
	    }
   }

   if (extent && image != NULL)
   	{

	   new_image = GetExtentImage(image,width,height,exception);
	   if(new_image == (Image*) NULL)
	   {
    	   CatchException(exception);
    	   return image;
       }
	   else
	   {
	       DestroyImage(image);
           image=new_image;
		   if(pa->bgc[0] != '\0')
		   	{
		   	   unsigned short a,b,c;
			   sscanf(pa->bgc,"%hu-%hu-%hu",&a,&b,&c);
			   image->background_color.red = a;
			   image->background_color.green = b;
			   image->background_color.blue = c;
		   	}
	    }
	 }

  if (strip){
    (void) StripImage(image);
  }

   image = new_image;

   return image;
}

int getParam(char *str,param* out)
{
	char * p = str;
	char * tmp = NULL;
	char buff[BUFF_SIZE] = {0};

   if(str[0] != '\0' && !strstr(str,"wh"))
   	{
	   if((strcmp(out->des_ext,out->src_ext)== 0) && str[0] == '\0' )
	   	  return 0;
	   else if(str[0] != '\0')
	   {
	      if(!regexMatch(str))
	   	    {out->enable = 0;
			return 1;
			}
	   }
	   else
	   	{
	   	   out->format = 1;
	   	}
   	}


   if(strstr(str,"watermark"))
   	{
   	   strcpy(out->wartermark,str);
	   return 0;
   	}

   char *pstr = NULL;

   if((pstr=strstr(str,"ic")))
   	{
   	   strncpy(out->ic,str,pstr - str);
	   return 0;
   	}
   	 if((pstr=strstr(str,"ff")))
   	{
   	   out->ff=1;
	   return 0;
   	}

	 if((pstr=strstr(str,"is")))
   	{
   	   out->scaleflag=1;
	   return 0;
   	}
        if((pstr=strstr(str,"le")))
   	{
   	   strncpy(buff,str,pstr - str);
           out->lvlflag = atoi(buff);
	   return 0;
   	}

   if((pstr=strstr(str,"rc")))
   	{
	   int rctemp=0;
	   char *ptemp = NULL;
	   char bufftemp[BUFF_SIZE] = {0};
	   if(pstr<=str)
	   	{
		out->enable = 0;
		return 1;
		}
		 unsigned long height=0;
		 unsigned long width=0;
		int gravity=0;
   	   	strncpy(out->rc,str,pstr-str);
	    if(out->rc[0] == 'x')
       	{
	       sscanf(out->rc,"x%lu-%d",&height,&gravity);
            if( height<1 ||height >4096|| gravity<1 || gravity>9)
			{
				out->enable = 0;
				return 1;
			}
       	}
	    else if(strstr(out->rc,"x-") != NULL )
		{
		   sscanf(out->rc,"%lux-%d",&width,&gravity);
		  if( width<1 ||width >4096|| gravity<1 || gravity>9)
			{
				out->enable = 0;
				return 1;
			}

		}
		else
		{
	       sscanf(out->rc,"%lux%lu-%d",&width,&height,&gravity);
		if( height<1 ||height >4096||width<1 ||width >4096|| gravity<1 || gravity>9)
			{
				out->enable = 0;
				return 1;
			}
		}
		return 0;
   	}


   if((pstr=strstr(str,"bgc")))
   	{
	strncpy(out->bgc,str,pstr-str);
	if (!regexBgcmh(out->bgc))
	   {
			 out->enable = 0;
	   		return 1;
	   }
	   return 0;
   	}


   if((pstr=strstr(str,"pr")))
   	{
   	   strncpy(buff,str,pstr-str);
	   out->progress = atoi(buff);
	   if (out->progress>1 || out->progress<0)
	   {
		out->enable = 0;
	   		return 1;
	   }
	   return 0;
   	}

   if((pstr=strstr(str,"wh")))
   	{
   	   strncpy(buff,str,pstr-str);
	   out->white= atoi(buff);
	   return 0;
   	}

   if((pstr=strstr(str,"sh")))
   	{
   	   strncpy(buff,str,pstr-str);
	   out->sharp= atoi(buff);
	   if(out->sharp >399 || out->sharp<50)
	   {
		    out->enable = 0;
	   		return 1;
	   }
	   else
	   {
			return 0;
	   }
   	}

		if((pstr=strstr(str,"rp")))
	{
   	   strncpy(buff,str,pstr-str);
	   out->strip= atoi(buff);
	   if(out->strip >1 || out->strip<0)
	   {
		    out->enable = 0;
	   		return 1;
	   }
	   else
	   {
			return 0;
	   }
   	}

   if((pstr=strstr(str,"bl")))
   	{
	   int ra=0;
	   int si=0;
	   char *ptemp = NULL;
	   char bufftemp[BUFF_SIZE] = {0};
	   if(pstr==NULL || pstr<=str)
	   	{
		out->enable = 0;
		return 1;
		}
		strncpy(out->blur,str,pstr-str);
	   ptemp=strstr(str,"-");
	   if(ptemp==NULL||ptemp<=str)
	   {
		out->enable = 0;
		return 1;
		}
		strncpy(bufftemp,str,ptemp-str);
	   	ra = atoi(bufftemp);
		if(ra > 50 || ra < 1)
	    {
	        out->enable = 0;
	   		return 1;
	    }
		ptemp++;
		if(pstr<=ptemp)
	    {
		out->enable = 0;
		return 1;
		}
		strncpy(bufftemp,ptemp,pstr-ptemp);
	   si = atoi(bufftemp);
	   if(si > 50 || si < 1)
	   {
	        out->enable = 0;
	   		return 1;
	   }
			return 0;
   	}

   while (p && *p != '\0')
   	{
   	   tmp = p;

	   while((*p >= '0' && *p <= '9') || *p == '-' || *p == 'x' || *p == 'X')
	   {
	   	    p++;
			continue;
	   }


	   if( *p == 'w' )
       {
          strncpy(buff, tmp  , p - tmp );
          out->width = atoi(buff);
		if(out->width  <= 0 || out->width>MAXSIZE)
	   	{
			out->enable = 0;
		   	return 1;
		}
	   	 else
	   	{
			return 0;
	   	}
       }

	   if( *p == 'h' )
       {
          strncpy(buff, tmp , p - tmp );
          out->height= atoi(buff);
		if(out->height  <= 0 || out->height > MAXSIZE)
	   	{
			out->enable = 0;
		   	return 1;
		}
	   	 else
	   	{
			return 0;
	   	}
       }

	   if( *p == 'r' )
       {
         strncpy(buff, tmp , p - tmp );
         out->rotate= atoi(buff);
		 if(out->rotate > 360 || out->rotate < 0)
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   	 else
	   		{
				return 0;
	   		}
       }


	   if ( *p == 'p' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->percent= atoi(buff);

			if(out->percent > 1000 || out->percent < 1)
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
				return 0;
	   		}

	   	}

	   if ( *p == 'e' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->edge= atoi(buff);
			if(out->edge > 4 || out->edge < 0 || out->edge == 3)
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
				return 0;
	   		}
	   	}

	   if ( *p == 'c' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->crop= atoi(buff);
			if(out->crop > 1 || out->crop < 0)
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
				return 0;
	   		}
	   	}
	   if ( *p == 'q' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->quality= atoi(buff);
			if(out->quality > 100 || out->quality < 1)
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
				return 0;
	   		}
	   	}
	   if ( *p == 'Q' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->abquality= atoi(buff);
			if(out->abquality > 100 || out->abquality < 1)
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
				return 0;
	   		}
	   	}

	   if ( *p == 'o' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->orientation= atoi(buff);
		   	if(out->orientation > 2 || out->orientation < 0 )
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
			    if(out->orientation==0)
				{
					out->orientation=3;                  //for remove exif
				}
				return 0;
	   		}

	   	}

	   if ( *p == 'a' )
	   	{
	   	   strncpy(out->adcrop,tmp,p - tmp);
			unsigned long width = 0;
			unsigned long height = 0;
			int x=0,y=0;
			int num=0;
      		num=sscanf(out->adcrop,"%d-%d-%ld-%ld",&x,&y,&width,&height);
			if (num< 4) {
				out->enable = 0;
		   		return 1;
			}
			if (width<1 ||width >4096 || height<1 || height>4096)
			{
				out->enable = 0;
		   		return 1;
			}
	   	}

	   if ( *p == 'b' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->brightness= atoi(buff);
			if(out->brightness > 100 || out->brightness < -100)
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
				return 0;
	   		}

	   	}

	   	if ( *p == 'd' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->contrast= atoi(buff);
			if(out->contrast > 100 || out->contrast < -100)
	   		{
			    out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
				return 0;
	   		}
	   	}

	    if ( *p == 'l' )
	   	{
		   strncpy(buff, tmp , p - tmp );
		   out->large= atoi(buff);
			if(out->large > 1 || out->large < 0)
	   		{
				out->enable = 0;
		   		return 1;
		   	}
	   		else
	   		{
				return 0;
	   		}
	   	}


       p++;

   	}
	return 0;
}

int getParams(char *str, param* out)
{
    char buff[BUFF_SIZE] = {0};
    int result=0;

    char *p = str;
	char *substr = NULL;
	while((substr = strchr(p,FORMAT_SEPERATOR)))
	{
	    memset(buff,0,sizeof(buff));
	    strncpy(buff, p, substr - p);

		result=getParam(buff,out);
		if(0!=result)
		{
		    return result;
		}
		p = substr + 1;
	}

	memset(buff,0,BUFF_SIZE);
	strcpy(buff,p);
	result=getParam(buff,out);
	return result;
}

int getParamFromInput(char * srcext, char* format, param * out1)
{
	char str1[BUFF_SIZE] = {0};
    int result=0;
    out1->enable = 1;

        strncpy(out1->src_ext, srcext,strlen(srcext));
	if(!strcmp(srcext,"pdf") ){    //pdf default quality
            out1->quality=90;
            out1->abquality=90;
	}


	if(strcmp(format,"exif") == 0)
	{
	   out1->exif = 1;
	   return 0;
	}

	if(strcmp(format,"infoexif") == 0)
	{
	   out1->exif = 2;
	   return 0;
	}
	strncpy(str1,format,strlen(format));

	char *pExt1 = strchr(str1,'.');

	if (pExt1 != NULL )
	{
	   if (( str1 + strlen(str1) - pExt1) < EXT_LEN)
	   {
	     strncpy(out1->des_ext,pExt1 + 1,str1 + strlen(str1) - pExt1);
	     memset(pExt1, 0 ,strlen(pExt1));
		if (!regexExtmh(out1->des_ext))
		{
		    out1->enable=0;
			return 0;
		}
	   }
	   else
	   {
		  return 0;
	   }
	}
	else
	{
	   strncpy(out1->des_ext,out1->src_ext,strlen(out1->src_ext));
    }
	if(regexWidthHeigthMatch(format))
	{
	   out1->whflag=1;
	}
	result=getParams(str1,out1);
    return result;
}


int regexMatch(char *src)
{
     char errbuf[1024];
	 regex_t reg;
	 int err=0,nm = 10;
	 char *pattern = REG_PATTERN;
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

int regexExtmh(char *src)
{
     char errbuf[1024];
	 regex_t reg;
	 int err=0,nm = 10;
	 char *pattern = EXTMH;
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
int regexBgcmh(char *src)
{
     char errbuf[1024];
	 regex_t reg;
	 int err=0,nm = 10;
	 char *pattern = BGCMH;
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
int regexWidthHeigthMatch(char *src)
{
     char errbuf[1024];
	 regex_t reg;
	 int err=0,nm = 10;
	 char *pattern = WIDTHHEIGTH;
	 regmatch_t pmatch[nm];

     if(regcomp(&reg,pattern,REG_EXTENDED) < 0){
         regerror(err,&reg,errbuf,sizeof(errbuf));
         printf("err:%s\n",errbuf);
		 return false;
     }

     err = regexec(&reg,src,nm,pmatch,0);

    if(err == REG_NOMATCH){
	   printf("str=%s \t",src);
	   printf("regexWidthHeigthMatch no match\n");
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

void * cresize(char * ext,void * data ,int* dataLen,int width,int height)
{
  ExceptionInfo exception;
  char geoBuf[40];
  Image *image,*resize_image=NULL;
   RectangleInfo geometry;
  ImageInfo *image_info;
   size_t ldataLen=0;
  GetExceptionInfo(&exception);
  image_info=CloneImageInfo((ImageInfo *) NULL);
  image = BlobToImage(image_info,data,*dataLen,&exception);

  if (!image) {
     printf("Failed to read any images!\n");
     goto done;

  }
  sprintf(geoBuf,"%dx%d>",width,height);
   (void) GetImageGeometry(image,geoBuf,MagickTrue,&geometry);
    if ((geometry.width == image->columns) && (geometry.height == image->rows)){
      resize_image=image;
    }else{
      resize_image=ScaleImage(image,geometry.width,geometry.height,&exception);
  }
  if (resize_image == (Image *) NULL){
         printf("Failed to resize images!\n");
         CatchException(&exception);
         goto done;
  }
 (void) strncpy(resize_image->magick,ext,sizeof(resize_image->magick));
 (void) strncpy(resize_image->filename,"",sizeof(resize_image->filename));

  data = ImageToBlob(image_info,resize_image,&ldataLen,&exception);

  if (!data) {
       printf("Failed to write images!\n");
       CatchException(&exception);
       goto done;
  }
done:
  if (image){
   DestroyImageList(image);
   }
  if (resize_image && resize_image != image){
   DestroyImageList(resize_image);
  }
  DestroyImageInfo(image_info);
  DestroyExceptionInfo(&exception);
  *dataLen = ldataLen;
  return data;
}
void * cmogrifyimage(char * src_ext,char ** des_ext,void * data ,int* dataLen,int width,int height,int quality,int strip,int filler,int isLarge,int extent )
{
  ExceptionInfo exception;
  char geoBuf[40];
  Image *image,*resize_image=NULL;
   RectangleInfo geometry;
  ImageInfo *image_info;
   size_t ldataLen=0;
  GetExceptionInfo(&exception);
  image_info=CloneImageInfo((ImageInfo *) NULL);
  image = BlobToImage(image_info,data,*dataLen,&exception);

  if (!image) {
     printf("Failed to read any images!\n");
     data = NULL;
     goto done;

  }
  if (quality > 0 && quality <= 100)
  {
	if(quality < image_info->quality)
	{
       image_info->quality = quality;
	}
  }
   image->gravity = CenterGravity;
   if (width > 0 || height > 0){
      if (width <= 0){
        sprintf(geoBuf,"x%d",height);
      }else if (height <=0){
        sprintf(geoBuf,"%dx",width);
      }else{
   if (filler){
     if (isLarge){
     sprintf(geoBuf,"%dx%d",width,height);
     }else{
     sprintf(geoBuf,"%dx%d>",width,height);
     }
   }else{
     if (isLarge){
      sprintf(geoBuf,"%dx%d^",width,height);
     }else{
      sprintf(geoBuf,"%dx%d>^",width,height);
     }

   }
	   }
   (void) GetImageGeometry(image,geoBuf,MagickTrue,&geometry);
      resize_image=ScaleImage(image,geometry.width,geometry.height,&exception);
   if (resize_image == (Image *) NULL){
         data = NULL;
         printf("Failed to resize images!geo=%s\n",geoBuf);
         CatchException(&exception);
         goto done;
  }
    DestroyImageList(image);
    image=resize_image;
    image->gravity = CenterGravity;
    }
  if (extent && (width > 0 || height > 0)){
	    Image  *extent_image;
      if (width <= 0){
        sprintf(geoBuf,"x%d",height);
      }else if (height <=0){
        sprintf(geoBuf,"%dx",width);
      }else{
	        sprintf(geoBuf,"%dx%d",width,height);
	   }
            (void) GetImageGeometry(image,geoBuf,MagickFalse,&geometry);
            if (geometry.width == 0)
              geometry.width=image->columns;
            if (geometry.height == 0)
              geometry.height=image->rows;
            geometry.x=(-geometry.x);
            geometry.y=(-geometry.y);
            extent_image=ExtentImage(image,&geometry,&exception);
            if (extent_image == (Image *) NULL){
               printf("Failed to extent images!\n");
               data = NULL;
              goto done;

             }
            DestroyImageList(image);
            image=extent_image;

	  }
  if (strip){
    (void) StripImage(image);
  }

    if(strcmp(*des_ext,"webp")== 0)
	{
		if( image->columns < WEBP_MAX_SIZE && image->rows < WEBP_MAX_SIZE )
		{
			 (void) strncpy(image->magick,*des_ext,sizeof(image->magick));
		}
		else
		{
			(void) strncpy(image->magick,src_ext,sizeof(image->magick));
			if(*des_ext!=NULL)
			{ (void) CloneString(des_ext,image->magick);}
		}
	}
 //(void) strncpy(image->magick,ext,sizeof(image->magick));
 (void) strncpy(image->filename,"",sizeof(image->filename));

  data = ImageToBlob(image_info,image,&ldataLen,&exception);

  if (!data) {
       printf("Failed to write images!\n");
       CatchException(&exception);
       goto done;
  }
done:
  if (image){
   DestroyImageList(image);
   }
  DestroyImageInfo(image_info);
  DestroyExceptionInfo(&exception);
  *dataLen = ldataLen;
  return data;
}
*/
import "C"
import (
	"net/http"
	"strings"
	"unsafe"
)
import _ "fmt"
import _ "os"
import _ "strconv"
import "regexp"
import "code.google.com/p/weed-fs/go/public"

func Resized(ext string, data []byte, width, height int) (resized []byte, w int, h int) {
	if width == 0 && height == 0 {
		return data, 0, 0
	}
	if width <= 0 {
		width = height
	}
	if height <= 0 {
		height = width
	}
	cext := C.CString(ext[1:])
	cdatalen := C.int(len(data))
	cwidth := C.int(width)
	cheight := C.int(height)
	cdata := unsafe.Pointer(&data[0])

	cresizeData := C.cresize(cext, cdata, &cdatalen, cwidth, cheight)

	// str := C.GoStringN((*C.char)(cresizeData),cdatalen)
	resized = C.GoBytes(unsafe.Pointer(cresizeData), cdatalen)
	C.free(cresizeData)
	C.free(unsafe.Pointer(cext))
	return resized, width, height
}

func GMInit(path string) {
	cpath := C.CString(path)
	C.InitializeMagick(cpath)
	DestroySign()
	C.free(unsafe.Pointer(cpath))
}
func GMDestroy() {
	C.DestroyMagick()
}
func DestroySign() {
	var new_sig_set, old_sig_set C.sigset_t

	if C.sigemptyset(&new_sig_set) != 0 {
		return
	}
	if C.sigemptyset(&old_sig_set) != 0 {
		return
	}
	if C.sigaddset(&new_sig_set, C.SIGTERM) != 0 {
		return
	}
	if C.sigprocmask(C.SIG_BLOCK, &new_sig_set, &old_sig_set) != 0 {
		return
	}
}

func MogrifyImage1(srcext, dstext, format string, data []byte, maxColRowSize int) (resized []byte, outext string, scaleflag bool, status int, lvlflag int) {

	scaleflag = false
	lvlflag = 0
	match, _ := regexp.MatchString("^[1-3]le_is$|^is_[1-3]le$|^is$", format)
	if match == true {
		if dstext == "" {
			outext = srcext
		} else {
			outext = dstext
		}
		status = 200
		resized = data
		if strings.Contains(format, "1le") {
			lvlflag = 1
		} else if strings.Contains(format, "2le") {
			lvlflag = 2
		} else if strings.Contains(format, "3le") {
			lvlflag = 3
		}

		if strings.ToLower(outext) == "jpg" || strings.ToLower(outext) == "jpeg" || strings.ToLower(outext) == "png" {
			scaleflag = true
		}
		return resized, outext, scaleflag, status, lvlflag
	}
	if srcext == dstext && format == "" {
		return data, "", false, http.StatusBadRequest, lvlflag
	}
	csrcext := C.CString((srcext))
	cdesext := C.CString(dstext)
	cformat := C.CString((format))
	cdatalen := C.int(len(data))
	//cwidth := C.int(width)
	//cheight := C.int(height)
	//cquality := C.int(quality)
	//cstrip  := C.int(strip)
	//cfiller := C.int(filler)
	//cisLarge := C.int(isLarge)
	//cextent := C.int(extent)
	cdata := unsafe.Pointer(&data[0])
	cmaxsize := C.int(maxColRowSize)

	cstatus := C.int(status)
	cscaleflag := C.int(0)
	clvlflag := C.int(0)

	cframelimt := C.int(public.FrameLimt)
	cgifproduct := C.int(public.GifProduct)

	cresizeData := C.getImgData(&cdatalen, cdata, cformat, csrcext, cmaxsize, &cstatus, &cdesext, &cscaleflag, &clvlflag, cframelimt, cgifproduct)
	if cresizeData != nil {
		resized = C.GoBytes(unsafe.Pointer(cresizeData), cdatalen)
		if cresizeData != cdata {
			C.free(cresizeData)
		}
	} else {
		resized = nil
	}
	outext = C.GoString(cdesext)
	C.free(unsafe.Pointer(cdesext))
	C.free(unsafe.Pointer(csrcext))
	C.free(unsafe.Pointer(cformat))

	status = int(cstatus)
	retflag := int(cscaleflag)
	lvlflag = int(clvlflag)
	if retflag != 0 {
		if outext == "jpg" || outext == "jpeg" || outext == "png" {
			scaleflag = true
		}
	}

	return resized, outext, scaleflag, status, lvlflag
}

func MogrifyImage(srcext, dstext string, data []byte, width, height, quality int, strip, filler, isLarge, extent int) (resized []byte, w int, h int, outext string) {

	if srcext == dstext && width == 0 && height == 0 && quality == -1 && strip == 0 && filler == 0 && isLarge == 0 && extent == 0 {
		return data, 0, 0, dstext
	}
	des_ext := C.CString(dstext)
	src_ext := C.CString(srcext)
	cdatalen := C.int(len(data))
	cwidth := C.int(width)
	cheight := C.int(height)
	cquality := C.int(quality)
	cstrip := C.int(strip)
	cfiller := C.int(filler)
	cisLarge := C.int(isLarge)
	cextent := C.int(extent)
	cdata := unsafe.Pointer(&data[0])

	cresizeData := C.cmogrifyimage(src_ext, &des_ext, cdata, &cdatalen, cwidth, cheight, cquality, cstrip, cfiller, cisLarge, cextent)

	// str := C.GoStringN((*C.char)(cresizeData),cdatalen)
	if cresizeData != nil {
		resized = C.GoBytes(unsafe.Pointer(cresizeData), cdatalen)
		C.free(cresizeData)
	} else {
		resized = nil
	}
	outext = C.GoString(des_ext)
	C.free(unsafe.Pointer(src_ext))
	C.free(unsafe.Pointer(des_ext))
	return resized, width, height, outext
}

/*
     (void) strcpy(image_info->magick,ext);


    resize_image=ResizeImage(image,width,height,PointFilter,1.0,&exception);
  resize_image=ResizeImage(image,width,height,LanczosFilter,1.0,&exception);
    resize_image=ScaleImage(image,width,height,&exception);
      printf(" write images %d!\n",*dataLen);
*/
