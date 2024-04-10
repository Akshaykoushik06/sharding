package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

var DBs []*sql.DB

func init() {
	username := "akshaykoushik"
	password := "@ABDevilliers17"
	host := "akshaysqlserver.mysql.database.azure.com"
	port := 3306
	database := "testdb"

	// build the DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)
	// Open the connection
	_db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	DBs = append(DBs, _db)

	_db2, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	DBs = append(DBs, _db2)
}

// implementation of static mapping
func getShardIndex(userID string) int {
	var index int = 0
	if userID == "1" {
		index = 0
	} else if userID == "2" {
		index = 1
	}

	fmt.Println("using DB", index)
	return index
}

func main() {
	ge := gin.Default()

	ge.POST("/heartbeats", func(ctx *gin.Context) {
		data := map[string]interface{}{}
		ctx.Bind(&data)

		userID := data["user_id"]
		DB := DBs[getShardIndex(strconv.Itoa(int(userID.(float64))))]

		if _, err := DB.Exec("REPLACE INTO oo_heartbeats (user_id, last_hb) VALUES (?, ?);", userID, time.Now().Unix()); err != nil {
			panic(err)
		}

		ctx.JSON(200, map[string]interface{}{"message": "ok"})
	})

	ge.GET("/heartbeats/status/:user_id", func(ctx *gin.Context) {
		var lastHB int

		userID := ctx.Param("user_id")
		DB := DBs[getShardIndex(userID)]

		row := DB.QueryRow("SELECT last_hb FROM oo_heartbeats WHERE user_id = ?;", userID)
		row.Scan(&lastHB)
		ctx.JSON(200, map[string]interface{}{"is_online": lastHB > int(time.Now().Unix())-30})
	})

	ge.Run(":9000")
}
