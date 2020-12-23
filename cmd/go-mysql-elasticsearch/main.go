package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql-elasticsearch/river"
)

//通过命令行读取配置
var configFile = flag.String("config", "./etc/river.toml", "go-mysql-elasticsearch config file")
var my_addr = flag.String("my_addr", "", "MySQL addr")
var my_user = flag.String("my_user", "", "MySQL user")
var my_pass = flag.String("my_pass", "", "MySQL password")
var es_addr = flag.String("es_addr", "", "Elasticsearch addr")

//数据存储目录
var data_dir = flag.String("data_dir", "", "path for go-mysql-elasticsearch to save data")

//canal配置项
var server_id = flag.Int("server_id", 0, "MySQL server id, as a pseudo slave")
var flavor = flag.String("flavor", "", "flavor: mysql or mariadb")
var execution = flag.String("exec", "", "mysqldump execution path")

var logLevel = flag.String("log_level", "info", "log level")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	log.SetLevelByName(*logLevel)

	//创建信号channel
	sc := make(chan os.Signal, 1)
	//配置channel监听哪些信号
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	//初始化配置文件
	cfg, err := river.NewConfigWithFile(*configFile)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	//命令行参数替换配置文件参数
	if len(*my_addr) > 0 {
		cfg.MyAddr = *my_addr
	}

	if len(*my_user) > 0 {
		cfg.MyUser = *my_user
	}

	if len(*my_pass) > 0 {
		cfg.MyPassword = *my_pass
	}

	if *server_id > 0 {
		cfg.ServerID = uint32(*server_id)
	}

	if len(*es_addr) > 0 {
		cfg.ESAddr = *es_addr
	}

	if len(*data_dir) > 0 {
		cfg.DataDir = *data_dir
	}

	if len(*flavor) > 0 {
		cfg.Flavor = *flavor
	}

	if len(*execution) > 0 {
		cfg.DumpExec = *execution
	}

	//创建river
	r, err := river.NewRiver(cfg)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	//不明白为什么要创建这个done channel
	done := make(chan struct{}, 1)
	go func() {
		//在协程中执行river.Run方法 r.wg.Add(1)
		r.Run()
		done <- struct{}{}
	}()

	//阻塞监听退出信号
	select {
	//监听sc信号量channel，响应用户退出
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	//监听Contet的Done事件，响应程序内部的退出，目前看done事件在r.Close里执行,以及sync.go里有3处触发
	//1.errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
	//2.log.Errorf("do ES bulk err %v, close sync", err)
	//3.log.Errorf("save sync position %s err %v, close sync", pos, err)
	//关闭流程: 上面3处任一错误，执行 ctx.cancel() 触发 ctx.Done , 执行 syncLoop 函数的 defer ticker.Stop(); defer r.wg.Done(); return
	case <-r.Ctx().Done():
		log.Infof("context is done with %v, closing", r.Ctx().Err())
	}

	//执行river.Close() r.cancel()、r.canal.Close()、r.master.Close()、r.wg.Wait()
	r.Close()
	<-done
}
