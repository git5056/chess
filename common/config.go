package common

import (
	"io"

	"github.com/chess/util/conf"
)

var config struct {
	Gateid     uint32 `ini:"gateid"`
	ListenPort int    `ini:"listen_port"`
	CenterAddr string `ini:"center_addr"` // server_center address
	TableAddr  string `ini:"table_addr"`  // server_table address
	RedisAddr  string `ini:"redis_addr"`  // redis address
	UserAddr   string `ini:"user_addr"`   // user db address
}

type EConfig struct {
	Gateid     uint32 `ini:"gateid"`
	ListenPort int    `ini:"listen_port"`
	CenterAddr string `ini:"center_addr"` // server_center address
	TableAddr  string `ini:"table_addr"`  // server_table address
	RedisAddr  string `ini:"redis_addr"`  // redis address
	UserAddr   string `ini:"user_addr"`   // user db address
}

func GetConfig() EConfig {
	return EConfig{
		Gateid:     config.Gateid,
		ListenPort: config.ListenPort,
		CenterAddr: config.CenterAddr,
		TableAddr:  config.TableAddr,
		RedisAddr:  config.RedisAddr,
		UserAddr:   config.UserAddr,
	}
}

func InitConfigWithBytes(reader io.Reader) error {
	return conf.LoadJson(reader, &config)
}

func InitConfig(confFile string) error {
	return conf.LoadIniFromFile(confFile, &config)
}

func GetGateid() uint32 {
	return config.Gateid
}

func SetGateid(gateid uint32) {
	config.Gateid = gateid
}

func GetListenPort() int {
	return config.ListenPort
}

func GetCenterAddr() string {
	return config.CenterAddr
}

func GetTableAddr() string {
	return config.TableAddr
}

func GetRedisAddr() string {
	return config.RedisAddr
}

func GetUserAddr() string {
	return config.UserAddr
}
