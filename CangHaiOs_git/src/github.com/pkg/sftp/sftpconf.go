package sftp

import (
	"strconv"
)

const (
	FTPVERSION = 2
)

type SftpConf struct {
	/*
		是否开启了，默认true
	*/

	Power bool

	/*
	   是否允许匿名登录FTP服务器，默认设置为YES允许
	   用户可使用用户名ftp或anonymous进行ftp登录，口令为用户的E-mail地址。
	   如不允许匿名访问则设置为NO
	*/
	Anonymous_enable bool

	/*
	   是否允许本地用户对FTP服务器文件具有写权限，默认设置为YES允许
	*/
	Write_enable bool

	/*
	   是否允许匿名用户上传文件，须将全局的write_enable=YES。默认为YES
	*/
	Anon_upload_enable bool

	/*
	     登录FTP服务器时显示的欢迎信息
	     如有需要，可在更改目录欢迎信息的目录下创建名为.message的文件，并写入欢迎信息保存后
	   Welcome to sdoss FTP service.
	*/
	Ftpd_banner string

	/*
	   设置数据传输中断间隔时间，此语句表示空闲的用户会话中断时间为600秒
	   即当数据传输结束后，用户连接FTP服务器的时间不应超过600秒。可以根据实际情况对该值进行修改
	*/
	Idle_session_timeout int

	/*
	   设置数据连接超时时间，该语句表示数据连接超时时间为120秒，可根据实际情况对其个修改
	*/
	Data_connection_timeout int

	/*
	   是否以ASCII方式传输数据。默认情况下，服务器会忽略ASCII方式的请求。
	   启用此选项将允许服务器以ASCII方式传输数据
	   不过，这样可能会导致由"SIZE /big/file"方式引起的DoS攻击
	*/
	Ascii_upload_enable bool

	Ascii_download_enable bool

	/*
	   是否允许递归查询。默认为关闭，以防止远程用户造成过量的I/O
	*/
	Ls_recurse_enable bool

	//文件夹最大显示文件数。默认是10000
	Max_show_sum int
	Version      string
}

func NewFtpConf() *SftpConf {
	fc := &SftpConf{
		Power:                   true,
		Anonymous_enable:        false,
		Anon_upload_enable:      false,
		Write_enable:            true,
		Ftpd_banner:             "Welcome To Sdoss FTP service",
		Idle_session_timeout:    600,
		Data_connection_timeout: 3000,
		Ascii_upload_enable:     false,
		Ascii_download_enable:   false,
		Ls_recurse_enable:       false,
		Max_show_sum:            10000,
		Version:                 "",
	}
	return fc
}
func (fc *SftpConf) Full(parms map[string]string) {
	if v, ok := parms["Power"]; ok {
		fc.Power = !(v == "false")
	}
	if v, ok := parms["Anonymous_enable"]; ok {
		fc.Anonymous_enable = (v == "true")
	}
	if v, ok := parms["Anon_upload_enable"]; ok {
		fc.Anon_upload_enable = (v == "true")
	}
	if v, ok := parms["Write_enable"]; ok {
		fc.Write_enable = !(v == "false")
	}
	if v, ok := parms["Ftpd_banner"]; ok {
		fc.Ftpd_banner = v
	}
	if v, ok := parms["Idle_session_timeout"]; ok {
		value, err := strconv.Atoi(v)
		if err == nil {
			fc.Idle_session_timeout = value
		}
	}
	if v, ok := parms["Data_connection_timeout"]; ok {
		value, err := strconv.Atoi(v)
		if err == nil {
			fc.Data_connection_timeout = value
		}
	}
	if v, ok := parms["Ascii_upload_enable"]; ok {
		fc.Ascii_upload_enable = (v == "true")
	}
	if v, ok := parms["Ascii_download_enable"]; ok {
		fc.Ascii_download_enable = (v == "true")
	}
	if v, ok := parms["Ls_recurse_enable"]; ok {
		fc.Ls_recurse_enable = (v == "true")
	}
	if v, ok := parms["Max_show_sum"]; ok {
		value, err := strconv.Atoi(v)
		if err == nil {
			fc.Max_show_sum = value
		}
	}
	if v, ok := parms["Version"]; ok {
		fc.Version = v
	}
}

//现有的配置形成新的配置文件
func (fc *SftpConf) ToBytes() []byte {
	constr := ""
	constr += "Power=" + strconv.FormatBool(fc.Power) + "\n"
	constr += "Anonymous_enable=" + strconv.FormatBool(fc.Anonymous_enable) + "\n"
	constr += "Anon_upload_enable=" + strconv.FormatBool(fc.Anon_upload_enable) + "\n"
	constr += "Write_enable=" + strconv.FormatBool(fc.Write_enable) + "\n"
	constr += "Ftpd_banner=" + fc.Ftpd_banner + "\n"
	constr += "Idle_session_timeout=" + strconv.Itoa(fc.Idle_session_timeout) + "\n"
	constr += "Data_connection_timeout=" + strconv.Itoa(fc.Data_connection_timeout) + "\n"
	constr += "Ascii_upload_enable=" + strconv.FormatBool(fc.Ascii_upload_enable) + "\n"
	constr += "Ascii_download_enable=" + strconv.FormatBool(fc.Ascii_download_enable) + "\n"
	constr += "Ls_recurse_enable=" + strconv.FormatBool(fc.Ls_recurse_enable) + "\n"
	constr += "Max_show_sum=" + strconv.Itoa(fc.Max_show_sum) + "\n"
	constr += "Version=" + fc.Version
	return []byte(constr)
}
