package main

import (
	"flag"
	"strings"
)

type configMapType map[string]string

var ConfigMap configMapType = map[string]string{}

func InitializeConfig() {
	dir := flag.String("dir", ".", "Directory containing db file")
	dbfilename := flag.String("dbfilename", "dump.rdb", "Database file")
	port := flag.String("port", "6379", "Port number")
	replicaOf := flag.String("replicaof", "", "set has replica of a master")

	flag.Parse()

	if *replicaOf != "" {
		splitedStr := strings.Split(*replicaOf, " ")
		*replicaOf = strings.Join(splitedStr, ":")
	}

	ConfigMap["dir"] = *dir
	ConfigMap["dbfilename"] = *dbfilename
	ConfigMap["port"] = *port
	ConfigMap["replicaOf"] = *replicaOf
	ConfigMap["masterID"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	ConfigMap["masterOffset"] = "0"
	ConfigMap["fullpath"] = ConfigMap["dir"] + "/" + ConfigMap["dbfilename"]
}

func (c *configMapType) IsSlave() bool {
	return ConfigMap["replicaOf"] != ""
}
