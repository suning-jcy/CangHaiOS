/** 
 * Read the Confuration file 
 * 
 * @copyright           (C) 2014  widuu 
 * @lastmodify          2014-2-22 
 * @website		http://www.widuu.com 
 * 
 */ 
package glog

import ( 
        //"code.google.com/p/weed-fs/go/glog"
 	"bufio" 
 	"fmt" 
 	"io" 
 	"os" 
 	"strings" 
        "strconv"
 )

 
type Conf struct { 
 	filepath string                         //your ini file path directory+file 
 	conflist []map[string]map[string]string //Confuration information slice 
} 
 
 
//Create an empty Confuration file 
func SetConf(filepath string) *Conf { 
 	c := new(Conf) 
 	c.filepath = filepath 
 
 
 	return c 
} 
 
/* 
//To obtain corresponding value of the key values 
func (c *Conf) GetValue(section, name, defualtValue string) string { 
 	c.ReadList() 
 	conf := c.ReadList() 
 	for _, v := range conf { 
 		for key, value := range v { 
 			if key == section {
                               if value[name] != "" { 
 				  return (value[name]) 
                               }
 			} 
         } 
 	}
        //return "no value"
 	return defualtValue 
} 

//To obtain corresponding value of the key values 
func (c *Conf) GetValue(section, name, defaultValue string) (ret string) { 
 	c.ReadList() 
 	conf := c.ReadList() 
 	for _, v := range conf { 
 		for key, value := range v { 
 			if key == section { 
 				if value[name] != nil {
 				   return value[name]
                                }
 			} 
         } 
 	} 
    return
} 
*/

//To obtain corresponding value of the key values 
func (c *Conf) GetStringValue(section, name, defaultValue ,cmdValue string) string { 
 	if defaultValue != cmdValue {
           return cmdValue
        }
        c.ReadList() 
 	conf := c.ReadList() 
 	for _, v := range conf { 
 		for key, value := range v { 
 			if key == section { 
 				if value[name] != "" {
 				   return value[name]
 				}
 			} 
         } 
 	} 
 	//return "no value"
 	return defaultValue 
}

func (c *Conf) GetIntValue(section, name string, defaultValue ,cmdValue int) int { 
        if defaultValue != cmdValue {
           return cmdValue
        }
 	c.ReadList() 
 	conf := c.ReadList() 
 	for _, v := range conf { 
 		for key, value := range v { 
 			if key == section { 
 				if value[name] != "" {
 				   ret, err := strconv.Atoi(value[name])
 				   if err == nil {
 				      return ret
 				   }
 				}
 			} 
         } 
 	} 
 	//return "no value"
 	return defaultValue 
}

func (c *Conf) GetUintValue(section, name string, defaultValue ,cmdValue uint) uint { 
        if defaultValue != cmdValue {
           return cmdValue
        }
 	c.ReadList() 
 	conf := c.ReadList() 
 	for _, v := range conf { 
 		for key, value := range v { 
 			if key == section { 
 				if value[name] != "" {
 				   ret, err := strconv.ParseUint(value[name], 10 ,0)
 				   if err == nil {
 				      return uint(ret)
 				   }
 				}
 			} 
         } 
 	} 
 	//return "no value"
 	return defaultValue 
}

func (c *Conf) GetBoolValue(section, name string, defaultValue ,cmdValue bool) bool {
        if defaultValue != cmdValue {
           return cmdValue
        }
        c.ReadList()
        conf := c.ReadList()
        for _, v := range conf {
                for key, value := range v {
                        if key == section {
                                if value[name] == "true" {
                                   return true
                                }else{
                                   return false
                                }
                        }
         }
        }
        //return "no value"
        return defaultValue
}

//Set the corresponding value of the key value, if not add, if there is a key change 
func (c *Conf) SetValue(section, key, value string) bool { 
 	c.ReadList() 
 	data := c.conflist 
 	var ok bool 
 	var index = make(map[int]bool) 
 	var conf = make(map[string]map[string]string) 
 	for i, v := range data { 
 		_, ok = v[section] 
 		index[i] = ok 
 	}

 	i, ok := func(m map[int]bool) (i int, v bool) { 
 		for i, v := range m { 
 			if v == true { 
 				return i, true 
 			} 
 		} 
 		return 0, false 
 	}(index) 
 
 	if ok { 
 		c.conflist[i][section][key] = value 
 		return true 
 	} else { 
 		conf[section] = make(map[string]string) 
 		conf[section][key] = value 
 		c.conflist = append(c.conflist, conf) 
 		return true 
 	} 
 
 	return false 
 } 
 
 
//Delete the corresponding key values 
func (c *Conf) DeleteValue(section, name string) bool { 
 	c.ReadList() 
 	data := c.conflist 
 	for i, v := range data { 
 		for key, _ := range v { 
 			if key == section { 
 				delete(c.conflist[i][key], name) 
 				return true 
 			} 
 		} 
 	} 
 	return false 
 } 
 
 
 //List all the Confuration file 
 func (c *Conf) ReadList() []map[string]map[string]string { 
 
 
 	file, err := os.Open(c.filepath) 
 	if err != nil { 
 		CheckErr(err) 
 	} 
 	defer file.Close() 
 	var data map[string]map[string]string 
 	var section string 
 	buf := bufio.NewReader(file) 
 	for { 
 		l, err := buf.ReadString('\n') 
 		line := strings.TrimSpace(l) 
 		if err != nil { 
 			if err != io.EOF { 
 				CheckErr(err) 
 			} 
 			if len(line) == 0 { 
 				break 
 			} 
 		} 
 		switch { 
 		case len(line) == 0: 
 		case line[0] == '[' && line[len(line)-1] == ']': 
 			section = strings.TrimSpace(line[1 : len(line)-1]) 
 			data = make(map[string]map[string]string) 
 			data[section] = make(map[string]string) 
                case strings.IndexAny(line, "#") >= 0:
                        break
 		default: 
 			i := strings.IndexAny(line, "=") 
 			value := strings.TrimSpace(line[i+1 : len(line)]) 
                        value = strings.Trim(value, "\"")
 			data[section][strings.TrimSpace(line[0:i])] = value 
 			if c.uniquappend(section) == true { 
 				c.conflist = append(c.conflist, data) 
 			} 
 		} 
 
 	} 
 
 
 	return c.conflist 
 } 
 
 
 func CheckErr(err error) string { 
 	if err != nil { 
 		return fmt.Sprintf("Error is :'%s'", err.Error()) 
 	} 
 	return "Notfound this error" 
 } 
 
 
 //Ban repeated appended to the slice method 
 func (c *Conf) uniquappend(conf string) bool { 
 	for _, v := range c.conflist { 
 		for k, _ := range v { 
 			if k == conf { 
 				return false 
 			} 
 		} 
 	} 
 	return true 
 } 
 


