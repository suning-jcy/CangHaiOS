// +build !gm

package images

import (
	"bytes"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"

	"github.com/disintegration/imaging"
	"github.com/resize"
)

func Resized(ext string, data []byte, width, height int) (resized []byte, w int, h int) {
	if width == 0 && height == 0 {
		return data, 0, 0
	}
	if srcImage, _, err := image.Decode(bytes.NewReader(data)); err == nil {
		bounds := srcImage.Bounds()
		var dstImage *image.NRGBA
		if bounds.Dx() > width && width != 0 || bounds.Dy() > height && height != 0 {
			if width == height && bounds.Dx() != bounds.Dy() {
				dstImage = imaging.Thumbnail(srcImage, width, height, imaging.Lanczos)
				w, h = width, height
			} else {
				dstImage = imaging.Resize(srcImage, width, height, imaging.Lanczos)
			}
		} else {
			return data, bounds.Dx(), bounds.Dy()
		}
		var buf bytes.Buffer
		switch ext {
		case ".png":
			png.Encode(&buf, dstImage)
		case ".jpg", ".jpeg":
			jpeg.Encode(&buf, dstImage, nil)
		case ".gif":
			gif.Encode(&buf, dstImage, nil)
		}
		return buf.Bytes(), dstImage.Bounds().Dx(), dstImage.Bounds().Dy()
	}
	return data, 0, 0
}
func GMInit(path string) {

}
func GMDestroy() {
}

func MogrifyImage1(srcext, dstext, format string, data []byte, maxColRowSize int) (resized []byte, outext string, scaleflag bool, status int,lvlflag int) {
	return
}

func GetMontageImg(dataList [][]byte, num int, tile string) (resized []byte) {
    return
}

func GetWaterMark(data []byte, watermark []byte, p int, s int, x int, y int, r int, voffset int, isCompatible bool) (resized []byte, status int) {
    return
}

func GetTextMark(data []byte, text string, p int, fonttype string, color string, size int, x int, y int, angle int) (resized []byte) {
    return
}

func MogrifyImage(ext, dstext string, data []byte, width, height, quality int, strip, filler, isLarge, extent int) (resized []byte, w int, h int, outext string) {
	if width == 0 && height == 0 {
		return data, 0, 0, dstext
	}
	if srcImage, _, err := image.Decode(bytes.NewReader(data)); err == nil {
		bounds := srcImage.Bounds()
		var dstImage *image.NRGBA
		if bounds.Dx() > width && width != 0 || bounds.Dy() > height && height != 0 {
			if width == height && bounds.Dx() != bounds.Dy() {
				dstImage = imaging.Thumbnail(srcImage, width, height, imaging.Lanczos)
				//dstImage = imaging.Thumbnail(srcImage, width, height, imaging.Lanczos)
				w, h = width, height
			} else {
				dstImage = imaging.Resize(srcImage, width, height, imaging.Lanczos)
			}
		} else {
			return data, bounds.Dx(), bounds.Dy(), dstext
		}
		var buf bytes.Buffer
		switch ext {
		case "png":
			png.Encode(&buf, dstImage)
		case "jpg", "jpeg":
			jpeg.Encode(&buf, dstImage, nil)
		case "gif":
			gif.Encode(&buf, dstImage, nil)
		}
		return buf.Bytes(), dstImage.Bounds().Dx(), dstImage.Bounds().Dy(), dstext
	}
	return data, 0, 0, dstext
}
func Resized2(ext string, data []byte, width, height int) (resized []byte, w int, h int) {
	if width == 0 && height == 0 {
		return data, 0, 0
	}
	if srcImage, _, err := image.Decode(bytes.NewReader(data)); err == nil {
		bounds := srcImage.Bounds()
		var dstImage image.Image
		if bounds.Dx() > width && width != 0 || bounds.Dy() > height && height != 0 {
			if width == height && bounds.Dx() != bounds.Dy() {
				dstImage = resize.Thumbnail(uint(width), uint(height), srcImage, resize.NearestNeighbor)
				//dstImage = imaging.Thumbnail(srcImage, width, height, imaging.Lanczos)
				w, h = width, height
			} else {
				dstImage = resize.Resize(uint(width), uint(height), srcImage, resize.Lanczos3)
			}
		} else {
			return data, bounds.Dx(), bounds.Dy()
		}
		var buf bytes.Buffer
		switch ext {
		case ".png":
			png.Encode(&buf, dstImage)
		case ".jpg", ".jpeg":
			jpeg.Encode(&buf, dstImage, nil)
		case ".gif":
			gif.Encode(&buf, dstImage, nil)
		}
		return buf.Bytes(), dstImage.Bounds().Dx(), dstImage.Bounds().Dy()
	}
	return data, 0, 0
}
func GetImageProperty(data []byte) (info *ImageInfo, ret int){
	return
}