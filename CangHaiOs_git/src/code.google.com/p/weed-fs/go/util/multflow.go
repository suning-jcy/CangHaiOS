package util

import (
	"hash"
	"crypto/md5"
	"encoding/hex"
	"crypto/rand"
	"io"
	"net/textproto"
	"bytes"
	"net/url"
	"path"
	"fmt"
	"mime"
	"strings"
	"path/filepath"
	"code.google.com/p/weed-fs/go/glog"
	"net/http"
	"io/ioutil"
	"errors"
	"time"
)

//从远端获取的待文件信息
type Appendfile struct {
	//最后一个文件块/整个文件块
	Lastblock io.ReadCloser
	//文件类型、原本普通上传的文件还是分块上传的
	Ftype string
	//分块上传的旧的上传uploadid
	Uploadid string
	//最后一个分块的位置信息
	Fixidx int
	//文件带有的tag标签
	Tag string
	//最后一个块的大小
	Size int64
	//原本是分块上传的，分块大小
	Blocksize int64
}

//对流进行截断，不会进行数据的封装
//专门用于分块文件的上传，
//用于已知文件类型是分块上传的，按照指定的数据上传。
type Multflow struct {
	//数据流
	r io.Reader
	//偏移量
	offset int64
	//分块大小
	blocksize int64
	//第几轮
	turn int
	//是不是真的结束了。只有流里面传输过来EOF 才是真正的结束
	over bool
	//保存每个分片的MD5信息
	crcs []string
	//用于计算每个分片的MD5
	ctx hash.Hash
}

//输入的是文件流和分块大小，
func Newmultflow(read io.Reader, blocksize int64) *Multflow {
	mf := &Multflow{
		r:         read,
		turn:      1,
		over:      false,
		ctx:       md5.New(),
		blocksize: blocksize,
	}
	//假如不设置块大小的，默认是64M
	if mf.blocksize == 0 {
		mf.blocksize = 64 * 1024 * 1024
	}
	return mf
}

func (mf *Multflow) Reset() {
	mf.turn++
	mf.ctx = md5.New()
}

//重新构建 boundary、头、尾
func (mf *Multflow) Read(p []byte) (n int, err error) {
	defer func() {
		if n > 0 && err == nil {
			mf.ctx.Write(p)
		}
		if err == io.EOF {
			if len(mf.crcs) == mf.turn-1 {
				mf.crcs = append(mf.crcs, "")
				mf.crcs[mf.turn-1] = hex.EncodeToString(mf.ctx.Sum(nil))
			}
		}
	}()
	if mf.offset%mf.blocksize == 0 && mf.turn == int(mf.offset/mf.blocksize) {
		return 0, io.EOF
	}
	if mf.over {
		return 0, io.EOF
	}
	var dataerr error
	left := mf.blocksize - mf.offset%mf.blocksize //left不可能是0
	if left < int64(len(p)) {
		temp := make([]byte, left)
		m := 0
		m, dataerr = mf.r.Read(temp)
		if dataerr != nil && dataerr != io.EOF {
			return 0, dataerr
		}
		if dataerr == io.EOF {
			mf.over = true
		}
		if dataerr == nil {
			mf.offset += int64(m)
			n = copy(p, temp[:m])
			return
		} else {
			return 0, dataerr
		}

	} else {
		n, dataerr = mf.r.Read(p)
		if dataerr != nil && dataerr != io.EOF {
			return 0, dataerr
		}
		if dataerr == io.EOF {
			mf.over = true
		}
		if dataerr == nil {
			mf.offset += int64(n)
			return n, nil
		} else {
			return 0, dataerr
		}
	}

}

func (mf *Multflow) IsOver() bool {
	return mf.over
}

//获取到现在的偏移值。over之后可以当做文件总大小
func (mf *Multflow) GetOffset() int64 {
	return mf.offset
}

//获取第几轮了
func (mf *Multflow) GetTurn() int {
	return mf.turn
}

//获取第几轮了
func (mf *Multflow) GetCrcs() []string {
	return mf.crcs
}

func (mf *Multflow) GetBlocksize() int64 {
	return mf.blocksize
}

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

//这个结构体是给ftp/sftp 用的。实际上是对数据进行了封装，成为multpart形式发送给filer。
//文件的上传方式等需要由外部再次进行判定。
//2018-5-24 需改造成可以可以设置文件分块界限，同时可以设置文件的分块大小，还要能额外的提供数据头
type Tempflow struct {
	//一个旧块的数据链路，这个块不可能超过64MB ，需要在初始化的时候就直接把数据一次性读取光，长度需要算入offset
	r0    io.Reader
	over0 bool
	//分块上传时候的分块大小
	blocksize int64

	//进行分块的阈值，最开始一个默认缓存大小需要是这么大。不能超过64M
	multthreshold int64

	//数据链
	r io.Reader
	//唯一码
	boundary string
	//鉴权
	Header textproto.MIMEHeader
	//头部信息，由boundary等拼接
	headBuffer *bytes.Buffer
	//尾，由boundary等拼接，且一次完整上传，和头需要boundary一致
	endBuffer *bytes.Buffer
	//现在已经从数据链读取到的数据长度
	offset int64

	//用于重试时候用的。
	//内部保存一个完整的数据块
	retryBuffer *bytes.Buffer
	//预先截取的数据，最大可能是multthreshold。
	tempBuffer *bytes.Buffer
	//用于触碰，
	tentacleBuffer *bytes.Buffer
	//第几轮，以块大小为分割
	turn int
	//数据链是不是空了，是绝对判定
	over    bool
	isRetry bool
	once    bool
}

func (tf *Tempflow) GetOffset() int64 {
	return tf.offset
}

func (tf *Tempflow) SetOnce(v bool) {
	tf.once = v
}

/*
先取到第一桶水
这个err出错，无法重试
*/
func NewTempflow(filename string, reader io.Reader, isGzipped bool, mtype string, blocksize, multthreshold int64, r0 io.Reader) (flow *Tempflow, err error) {
	flow = &Tempflow{}
	flow.r0 = r0
	if blocksize == 0 {
		blocksize = int64(4 * 104 * 104)
	}
	flow.blocksize = blocksize

	if multthreshold == 0 {
		multthreshold = int64(64 * 1024 * 1024)
	}
	flow.multthreshold = multthreshold

	flow.Header = make(textproto.MIMEHeader)
	//filename
	filename, _ = url.QueryUnescape(filename)
	_, filename = path.Split(filename)

	flow.Header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		flow.Header.Set("Content-Type", mtype)
	}
	if isGzipped {
		flow.Header.Set("Content-Encoding", "gzip")
	}
	flow.tempBuffer = bytes.NewBufferString("")

	//flow.tentacleBuffer =

	flow.retryBuffer = bytes.NewBufferString("")
	flow.tentacleBuffer = bytes.NewBufferString("")

	flow.CreateBoundary()
	flow.r = reader
	offset := 0
	flow.over0 = false
	if flow.r0 == nil {
		glog.V(4).Infoln("flow.r0 == nil")
		flow.over0 = true
	}
	//先填充下预取数据,取multthreshold 大小
	buf := make([]byte, 4*1024)
	left := multthreshold
	glog.V(4).Infoln(multthreshold, blocksize)
	for left > 0 {
		if left < int64(4*1024) {
			buf = make([]byte, left)
		}
		var firstreader io.Reader
		if !flow.over0 {
			glog.V(4).Infoln("read from r0")
			firstreader = flow.r0
		} else {
			glog.V(4).Infoln("read from r")
			firstreader = flow.r
		}
		nr, er := firstreader.Read(buf)
		if nr > 0 {
			glog.V(4).Infoln("read  size", nr, "left:", left)
			left -= int64(nr)
			nw, ew := flow.tempBuffer.Write(buf[0:nr])
			glog.V(4).Infoln("new temp size:", flow.tempBuffer.Len())
			if nw > 0 {
				offset += nw
			}
			if ew != nil {
				return nil, ew
			}
			if nr != nw {
				err = io.ErrShortWrite
				return nil, err
			}
		}
		if er == io.EOF {
			if !flow.over0 {
				flow.over0 = true
				er = nil
			} else {
				flow.over = true
				break
			}
		}
		if er != nil {
			err = er
			return nil, err
		}
		if nr == 0 {
			time.Sleep(5*time.Millisecond)
		}
	}
	flow.isRetry = false

	return
}

//重新构建 boundary、头、尾
//每个分块开始前需要进行一次
func (tf *Tempflow) CreateBoundary() {
	if tf.offset%tf.blocksize != 0 {
		return
	}
	tf.boundary = randomBoundary()
	var b bytes.Buffer
	fmt.Fprintf(&b, "--%s\r\n", tf.boundary)
	for k, vv := range tf.Header {
		for _, v := range vv {
			fmt.Fprintf(&b, "%s: %s\r\n", k, v)
		}
	}
	fmt.Fprintf(&b, "\r\n")
	tf.headBuffer = &b

	var be bytes.Buffer
	fmt.Fprintf(&be, "\r\n--%s--\r\n", tf.boundary)
	tf.endBuffer = &be
	//把保留的上一个分块扔掉
	tf.retryBuffer.Reset()
	tf.turn++
}

//用于重试，头和尾,数据的起始位置
func (tf *Tempflow) Reset() {
	//重新生成头。避免信息流错误
	tf.boundary = randomBoundary()
	var b bytes.Buffer
	fmt.Fprintf(&b, "--%s\r\n", tf.boundary)
	for k, vv := range tf.Header {
		for _, v := range vv {
			fmt.Fprintf(&b, "%s: %s\r\n", k, v)
		}
	}
	fmt.Fprintf(&b, "\r\n")
	tf.headBuffer = &b

	var be bytes.Buffer
	fmt.Fprintf(&be, "\r\n--%s--\r\n", tf.boundary)
	tf.endBuffer = &be
	glog.V(0).Infoln("+++++++++++++++++", tf.tempBuffer.Len())

	tf.isRetry = true
}

func (tf *Tempflow) IsOver() bool {
	return tf.over
}

func (tf *Tempflow) Getsize() int64 {
	return tf.offset
}

//上传一次，一次上传会到一次eof。整个数据被分为多个会返回eof的分段，真正的数据结束需要以over标识来区分
func (tf *Tempflow) Upload(uploadUrl string, header map[string]string) (err error) {
	req, err := http.NewRequest("POST", uploadUrl, tf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+tf.boundary)

	for key, value := range header {
		req.Header.Set(key, value)
	}

	resp, post_err := client.Do(req)

	if post_err != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return post_err
	}

	ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == 403 {
		return errors.New("Error Authorization")
	}
	if resp.StatusCode == 404 {
		return errors.New("Error Not Exist")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		return errors.New("Error Unknow with code from remote:" + fmt.Sprint(resp.StatusCode))
	}
	return

}

func (tf *Tempflow) MakeRequest(uploadUrl string, method string, data []byte, header map[string]string) (resp *http.Response, err error) {
	req, err := http.NewRequest(method, uploadUrl, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	return client.Do(req)
}

//只要err不是io.EOF，就是有问题的。
func (tf *Tempflow) Read(p []byte) (n int, err error) {
	glog.V(5).Infoln("Tempflow Read:", tf.offset, tf.headBuffer.Len(), tf.endBuffer.Len(), tf.tempBuffer.Len(), tf.turn)
	/*-----------尾部没了，都是eof-------------------*/
	if tf.endBuffer.Len() == 0 {
		glog.V(4).Infoln("return eof")
		return 0, io.EOF
	}

	/*-----------有头，优先读头-------------------*/
	hlen := tf.headBuffer.Len()
	if hlen > 0 {
		//buffer 没读完,而且足够多
		return tf.headBuffer.Read(p)
	}

	var dataerr error
	fblen := tf.tempBuffer.Len()

	/*-----------重试动作，从retryBuffer中读取-------------------*/
	if tf.isRetry {
		glog.V(5).Infoln("retry! ", tf.turn, tf.offset)
		n, err = tf.retryBuffer.Read(p)
		//重试的内容读完了自动标识回去
		if err == io.EOF {
			glog.V(5).Infoln("goto readendbuffer ", tf.turn, tf.offset)
			goto readendbuffer
			//重试标识自动设置为false，下次再读取就不是重试了
		}
		return n, err
	}

	/*----------- tempBuffer不为空，空了之后才有预读的事-------------------*/
	if fblen > 0 {
		//分块大小末尾 到末尾了，读endbuffer去吧
		if int64(tf.turn)*tf.blocksize == tf.offset && !tf.once {
			glog.V(4).Infoln("goto readendbuffer ", tf.turn, tf.offset)
			goto readendbuffer
		} else {
			left := tf.blocksize - tf.offset%tf.blocksize
			if left < int64(len(p)) {
				p = make([]byte, left)
			}
			//buffer 没读完,而且足够多
			n, err = tf.tempBuffer.Read(p)
			if n > 0 {
				tf.offset += int64(n)
				//把读过的数据写入到retryBuffer，防止重新读
				tf.retryBuffer.Write(p[:n])
				glog.V(4).Infoln("tempBuffer.Read ok ", tf.turn, tf.offset)
				return n, err
			}
			//读出错了
			if err != nil && err != io.EOF {
				return 0, err
			}
			//读完了，继续走下面的流程
			err = nil
		}
	} else {
		/*-----------tempBuffer为空，需要判定是不是已经结束了。这时候的tentacleBuffer不可能有东西 -------------------*/
		if tf.IsOver() {
			glog.V(4).Infoln("goto readendbuffer  ", tf.turn, tf.offset)
			goto readendbuffer
		}
	}

	if tf.once {
		//一次性的，读完buf就直接end了
		goto readendbuffer
	}
	//正好在节点上，进行下触碰,
	if int(tf.offset/tf.blocksize) == tf.turn && int(tf.offset%tf.blocksize) == 0 {
		glog.V(4).Infoln("touch ", tf.turn, tf.offset)
		//tf.tentacleBuffer
		tentacledata := make([]byte, 1)
		nr := 0
		var er error
		for nr == 0 && er == nil {
			var tempreader io.Reader
			if !tf.over0 {
				tempreader = tf.r0
			} else {
				tempreader = tf.r
			}
			nr, er = tempreader.Read(tentacledata)
			if er == io.EOF {
				if !tf.over0 {
					//r0  eof了继续从r读取
					tf.over0 = true
					er = nil
					continue
				} else {
					tf.over = true
					glog.V(4).Infoln("goto readendbuffer ", tf.turn, tf.offset)
					//正好所有数据都eof了
					goto readendbuffer
				}
			}
			if er != nil {
				glog.V(0).Infoln("read error", er)
				return 0, er
			}
			tf.tentacleBuffer.Write(tentacledata)
		}
		glog.V(4).Infoln("goto readendbuffer ", tf.turn, tf.offset)
		//已经到节点处了直接读尾部就好。
		goto readendbuffer
	}

	//只有每次的开头才让读触碰的东西
	if tf.tentacleBuffer.Len() > 0 && int(tf.offset/tf.blocksize) == tf.turn-1 && int(tf.offset%tf.blocksize) == 0 {
		glog.V(4).Infoln("tf.tentacleBuffer.Len() > 0 ", tf.turn, tf.offset)
		n, err = tf.tentacleBuffer.Read(p)
		if n > 0 {
			tf.offset += int64(n)
			tf.retryBuffer.Write(p[:n])
		}
		return n, err
	}

	/*从r 和 r0 中读取，保证别读超界（分块边界）*/
	if int(tf.offset/tf.blocksize) == tf.turn-1 {

		left := tf.blocksize - tf.offset%tf.blocksize //left不可能是0
		var tempreader io.Reader
		if !tf.over0 {
			tempreader = tf.r0
			glog.V(4).Infoln(" read from r0", tf.turn, tf.offset)
		} else {
			tempreader = tf.r
			glog.V(4).Infoln(" read from r", tf.turn, tf.offset)
		}
		if left < int64(len(p)) {
			temp := make([]byte, left)
			m := 0
			m, dataerr = tempreader.Read(temp)
			if dataerr != nil && dataerr != io.EOF {
				return 0, dataerr
			}
			if dataerr == io.EOF {
				if !tf.over0 {
					//r0  eof了继续从r读取
					tf.over0 = true
				} else {
					tf.over = true
					glog.V(4).Infoln("goto readendbuffer", tf.turn, tf.offset)
					//正好所有数据都eof了
					goto readendbuffer
				}
			}
			tf.offset += int64(m)
			n = copy(p, temp[:m])
			tf.retryBuffer.Write(temp[:m])
			return

		} else {
			n, dataerr = tempreader.Read(p)
			if dataerr != nil && dataerr != io.EOF {
				return 0, dataerr
			}
			if dataerr == io.EOF {
				if !tf.over0 {
					//r0  eof了继续从r读取
					tf.over0 = true
				} else {
					tf.over = true
					glog.V(4).Infoln("goto readendbuffer", tf.turn, tf.offset)
					//正好所有数据都eof了
					goto readendbuffer
				}
			}
			tf.retryBuffer.Write(p[:n])
			tf.offset += int64(n)
			return n, nil
		}
	}
readendbuffer:
	if tf.endBuffer.Len() > 0 {
		return tf.endBuffer.Read(p)
	} else {
		if tf.isRetry {
			tf.isRetry = false
		}
		return 0, io.EOF
	}
}

func randomBoundary() string {
	var buf [30]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf[:])
}
