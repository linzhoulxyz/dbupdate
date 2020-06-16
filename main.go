package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/spf13/viper"
)

// 数据库配置
type DbConfig struct {
	Host     string
	Port     string
	User     string
	Pwd      string
	Name     string
	MysqlBin string
}

var configFileName = "config.yaml" // 默认配置文件名
var dbConfig DbConfig
var db *gorm.DB

func main() {
	loadConfig(configFileName)

	dbConfig = DbConfig{
		Host:     viper.GetString("db.host"),
		Port:     viper.GetString("db.port"),
		User:     viper.GetString("db.user"),
		Pwd:      viper.GetString("db.pwd"),
		Name:     viper.GetString("db.name"),
		MysqlBin: viper.GetString("db.mysql_bin"),
	}
	initDb(dbConfig)

	update()
}

func init() {
	flag.StringVar(&configFileName, "config", "config.yaml", "-config=xxx")
	flag.Parse()
}

// loadConfig load config
func loadConfig(configFileName string) {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")      // optionally look for config in the working directory
	err := viper.ReadInConfig()   // Find and read the config file
	if err != nil {               // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}

// initDb init db
func initDb(config DbConfig) {
	var err error
	dns := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8&parseTime=True", config.User, config.Pwd, config.Host, config.Port, config.Name)
	db, err = gorm.Open("mysql", dns)
	if err != nil {
		panic(fmt.Errorf("Fatal error connect database: %s \n", err))
	}

	db.LogMode(true)

	db.AutoMigrate(&DbUpdateRecord{})
}

// update find new change sql file and update db
func update() {
	dbUpdateFileList := getAlreadyUpdateFileList()
	localUpdateFileList := getAllChangeFileList()
	newUpdateFileList := getNewChangeFile(dbUpdateFileList, localUpdateFileList)

	if len(newUpdateFileList) == 0 {
		fmt.Println("[INFO] Nothing need update, Done.")
		return
	}

	doSqlUpdate(newUpdateFileList)
}

// getAlreadyUpdateFileList get all ready updated file from db
func getAlreadyUpdateFileList() map[string]struct{} {
	var updates []DbUpdateRecord
	db.Find(&updates)

	alreadyUpdate := make(map[string]struct{})
	for _, update := range updates {
		alreadyUpdate[update.UpdateFile] = struct{}{}
	}

	return alreadyUpdate
}

// getAllChangeFileList get all change sql file file from dir
func getAllChangeFileList() map[string]struct{} {
	var changeFileList = make(map[string]struct{})

	list, err := ioutil.ReadDir(".")
	if err != nil {
		panic(fmt.Errorf("Fatal error read change files: %s \n", err))
	}

	for _, f := range list {
		if f.IsDir() {
			continue
		}

		tmpFileName := f.Name()
		if !isSqlFile(tmpFileName) {
			continue
		}

		changeFileList[tmpFileName] = struct{}{}
	}

	return changeFileList
}

// isSqlFile check if file is a sql file
func isSqlFile(fileName string) bool {
	if len(fileName) == 0 {
		return false
	}

	ext := filepath.Ext(fileName)

	return strings.ToLower(ext) == ".sql"
}

// getNewChangeFile get new change file and sorted by filename
func getNewChangeFile(dbRecords map[string]struct{}, localFiles map[string]struct{}) []string {
	var newFiles []string

	if len(dbRecords) == 0 && len(localFiles) == 0 {
		return newFiles
	}

	// diff localFiles and dbRecords
	for f, _ := range localFiles {
		if _, ok := dbRecords[f]; ok {
			continue
		}

		newFiles = append(newFiles, f)
	}

	if len(newFiles) == 0 {
		return newFiles
	}

	// sort by ascii
	sort.Strings(newFiles)

	return newFiles
}

// doSqlUpdate import sql to db
func doSqlUpdate(newFiles []string) {
	if len(newFiles) == 0 {
		return
	}

	for _, f := range newFiles {
		fPath, err := filepath.Abs(filepath.Join(".", f))
		if err != nil {
			log.Fatalf("[ERR] Get File Path err: %v, %v, %v", f, fPath, err)
		}

		var cmd *exec.Cmd
		mysqlCmd := fmt.Sprintf("%s -h%s -P%v -u%s --password=%s %s < %s",
			dbConfig.MysqlBin, dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Pwd, dbConfig.Name, fPath)
		switch runtime.GOOS {
		case "linux":
			cmd = exec.Command("/bin/sh", "-c", mysqlCmd)
		case "windows":
			cmd = exec.Command("cmd", "/C", mysqlCmd)
		default:
			log.Fatalf("[ERROR] Unsupport OS: %+v\n", runtime.GOOS)
		}

		log.Printf("CMD: %v\n", cmd)
		var out, stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err = cmd.Run()
		if err != nil {
			log.Fatalf("[ERROR] Error executing query. Command Output: %+v\n: %+v, %v", out.String(), stderr.String(), err)
		} else {
			log.Printf("[INFO] Imported File: %s\n", f)
			// save record
			record := DbUpdateRecord{}
			record.UpdateFile = f
			if err := db.Create(&record).Error; err != nil {
				log.Fatalf("[ERROR] Ready success import db file, but save record fail. %s, %v", f, err)
			}
		}
	}

	fmt.Println("[OK] Success Import File Count:", len(newFiles))
	return
}
