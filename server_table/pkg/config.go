package pkg

import (
	"github.com/chess/common"
	"github.com/chess/util/log"
)

func Init(confPath string) bool {
	if err := common.InitConfig(confPath + "/table.conf"); err != nil {
		log.Error("init common config fail")
		return false
	}

	if !InitRoomConfig(confPath + "/room.csv") {
		return false
	}

	return true
}
