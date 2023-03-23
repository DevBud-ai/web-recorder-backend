package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-co-op/gocron"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  8192,
	WriteBufferSize: 8192,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var db *sql.DB

var mysqlDb *sql.DB

var user *string

func main() {

	// Argument Parsing
	user = flag.String("user", "", "# of iterations")
	flag.Parse()
	if user == nil || *user == "" {

		fmt.Println("Please provide a user name")
		os.Exit(0)
	}

	var err error
	db, err = sql.Open("sqlite3", "file.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	query := `
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER NOT NULL,
        event TEXT NOT NULL,
        user TEXT NOT NULL,
        data JSON NOT NULL,
        isUpdates BOOLEAN NOT NULL DEFAULT 0
    );
`
	_, err = db.Exec(query)
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}

	mysqlDb, err = sql.Open("mysql", "dbmasteruser:j^OyRV&(Y4M^[nN^9MnIz!:ROt*zvCZ<@tcp(ls-8376c85ab384dee2a19acbbed761900828912cca.cn8drsemmg5o.ap-south-1.rds.amazonaws.com)/dbbud_recorder")
	if err != nil {
		panic(err)
	}
	defer mysqlDb.Close()
	// cron jobs
	s := gocron.NewScheduler(time.UTC)
	s.Every("1m").Do(syncDB)

	s.StartAsync()

	http.HandleFunc("/log", homePage)
	//	http.HandleFunc("/ws", wsEndpoint)
	println("Server is running on port 8080")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func syncDB() {
	fmt.Println("Syncing DB")

	// Prepare the batch insert statement
	stmt, err := mysqlDb.Prepare("INSERT INTO logs (timebud, event, user, data, isUpdates) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	rows, err := db.Query("SELECT * FROM logs WHERE isUpdates = 0")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	//batchSize := 100
	//
	//// Define a buffer for the batch insert values
	//values := make([]interface{}, 0, batchSize*5) // 5 = number of columns in logs table

	var ids []int

	for rows.Next() {
		var (
			id        int
			timestamp string
			event     string
			user      string
			data      string
			isUpdates bool
		)
		if err := rows.Scan(&id, &timestamp, &event, &user, &data, &isUpdates); err != nil {
			log.Fatal(err)
		}

		// fmt.Println(id, timestamp, event, user, data, isUpdates)
		_, err := stmt.Exec(timestamp, event, user, data, isUpdates)
		if err != nil {
			fmt.Println("error:", err)
			return
		}

		ids = append(ids, id)

	}
	idStr := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids)), ","), "[]")

	fmt.Println(idStr)

	sm := "UPDATE logs SET isUpdates = true WHERE id IN (" + idStr + ")"
	fmt.Println(sm)

	// update the row
	updated, err := db.Exec(sm)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Println(updated.RowsAffected())

}

func homePage(w http.ResponseWriter, r *http.Request) {
	requestData := make(map[string]interface{})
	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	//fmt.Println(requestData["data"].(map[string]interface{})["action"])

	jsonString, err := json.Marshal(requestData)
	if err != nil {
		panic(err)
	}

	//fmt.Println(string(jsonString))

	// store to db
	query := "INSERT INTO logs (timestamp, event,data,user) VALUES (?, ?,?,?)"
	result, err := db.Exec(query, requestData["timestamp"], requestData["data"].(map[string]interface{})["action"], jsonString, user)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	// get the ID of the inserted row (if the table has an AUTOINCREMENT column)
	id, _ := result.LastInsertId()

	fmt.Printf("Inserted row with ID %d\n", id)

}

func wsEndpoint(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			fmt.Println(err)
			return
		}
		//fmt.Println(string(p))
		data := make(map[string]interface{})
		err = json.Unmarshal([]byte(string(p)), &data)
		if err != nil {
			fmt.Println("error:", err)
			return
		}

		//fmt.Println(data["data"].(map[string]interface{})["action"])
		//fmt.Println(data["timestamp"])

		query := "INSERT INTO logs (timestamp, event,data) VALUES (?, ?,?)"

		// execute the query with some values
		result, err := db.Exec(query, data["timestamp"], data["data"].(map[string]interface{})["action"], string(p))
		if err != nil {
			fmt.Println("error:", err)
			return
		}

		// get the ID of the inserted row (if the table has an AUTOINCREMENT column)
		id, _ := result.LastInsertId()

		fmt.Printf("Inserted row with ID %d\n", id)

		if err = conn.WriteMessage(messageType, p); err != nil {
			fmt.Println(err)
			return
		}
	}
}
