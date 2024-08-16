package main

import (
	"github.com/injoyai/minidb"
)

var (
	DB = minidb.New("./data/database", "default") //数据库
)

func main() {

	DB.Insert(Config{Group: "1", Key: "1", Parent: "0", Name: "1", Value: "1", Type: "1"})

}

type Config struct {
	ID     string      `json:"id" orm:"time"` //主键
	Group  string      `json:"group"`         //分组
	Key    string      `json:"key"`           //唯一标识
	Parent string      `json:"parent"`        //父级的ID
	Name   string      `json:"name"`          //字段名称
	Value  interface{} `json:"value"`         //字段值
	Type   string      `json:"type"`          //字段类型
}
