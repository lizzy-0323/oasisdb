package main

import (
	"oasisdb/internal/config"
	"oasisdb/internal/db"
	"oasisdb/internal/router"
)

func main() {
	// 初始化配置
	conf, err := config.NewConfig(".")
	if err != nil {
		panic(err)
	}

	// 初始化数据库
	db := &db.DB{}
	if err := db.Open(conf); err != nil {
		panic(err)
	}

	// 初始化路由
	r := router.New(db)

	// 启动服务器
	r.Run(":8080")
}
