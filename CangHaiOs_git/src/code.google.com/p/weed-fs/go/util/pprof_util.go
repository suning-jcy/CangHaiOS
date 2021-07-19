package util

import (
    "code.google.com/p/weed-fs/go/glog"
 	"runtime/pprof"
	"runtime"
    "net/http"
	"os"
)

var cpuProfile *os.File = nil
func  DebugHandler(w http.ResponseWriter, r *http.Request) {
    debugtype := r.FormValue("type")
    //cpuProfile := r.FormValue("filename")
    switch debugtype {
        case "lookup_heap":
            p := pprof.Lookup("heap")
            p.WriteTo(os.Stdout, 2)
        case "lookup_threadcreate":
            p := pprof.Lookup("threadcreate")
            p.WriteTo(os.Stdout, 2)
        case "lookup_block":
            p := pprof.Lookup("block")
            p.WriteTo(os.Stdout, 2)
        case "lookup_goroutine":
            p := pprof.Lookup("goroutine")
            p.WriteTo(os.Stdout, 2)
        case "start_cpuprof":
            if cpuProfile == nil {
                if f, err := os.Create("server.cpuprof"); err != nil {
                    glog.V(2).Infoln("start cpu profile failed: %v", err)
                } else {
                    glog.V(2).Infoln("start cpu profile")
                    pprof.StartCPUProfile(f)
                    cpuProfile = f
                }
            }
        case "stop_cpuprof":
            if cpuProfile != nil {
                pprof.StopCPUProfile()
                cpuProfile.Close()
                cpuProfile = nil
                glog.V(2).Infoln("stop cpu profile")
            }
        case "get_memprof":
            if f, err := os.Create("/tmp/server.memprof"); err != nil {
                glog.V(2).Infoln("record memory profile failed: %v", err)
            } else {
                runtime.GC()
                p := pprof.Lookup("heap")
                p.WriteTo(f, 1)
                f.Close()
                glog.V(2).Infoln("record memory profile")
            }
       }
 }