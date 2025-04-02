package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"ECHO":    echo,
	"CONFIG":  config,
	"KEYS":    keys,
	"INFO":    info,
}

type setVal struct {
	value   string
	timeout *time.Time
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

func echo(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'echo' command"}
	}

	return Value{typ: "bulk", bulk: args[0].bulk}
}

var Config = map[string]string{}
var ConfigMu = sync.RWMutex{}

func config(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'config' command"}
	}

	command := strings.ToUpper(args[0].bulk)

	switch command {
	case "GET":
		if len(args) == 1 {
			return configGetAll()
		}
		return configGet(args[1:])
	case "SET":
		return configSet(args[1:])
	}

	return Value{typ: "error", str: "ERR error has occured with the 'config' command"}
}

func configGet(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'config get' command"}
	}

	fmt.Printf("args: %#v\n", args)
	k := args[0].bulk
	ConfigMu.Lock()
	defer ConfigMu.Unlock()
	val, ok := Config[k]

	if !ok {
		return Value{typ: "null"}
	}

	fmt.Printf("Config %+v\n", Config)

	arrVal := Value{typ: "array"}
	arrVal.array = append(arrVal.array, Value{typ: "bulk", bulk: k})
	arrVal.array = append(arrVal.array, Value{typ: "bulk", bulk: val})
	fmt.Printf("arrVal: %#v\n", arrVal)
	return arrVal
}

func configGetAll() Value {
	result := Value{typ: "array"}

	ConfigMu.RLock()
	defer ConfigMu.RUnlock()

	for k, v := range Config {
		result.array = append(result.array, Value{typ: "bulk", bulk: k})
		result.array = append(result.array, Value{typ: "bulk", bulk: v})
	}

	return result
}

func configSet(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	ConfigMu.Lock()
	Config[key] = value
	ConfigMu.Unlock()

	return Value{typ: "string", str: "OK"}

}

var SETs = map[string]setVal{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	var value setVal
	key := args[0].bulk

	if len(args) == 4 {
		command := strings.ToUpper(args[2].bulk)

		if command == "PX" {
			t_out, err := strconv.Atoi(args[3].bulk)

			if err != nil {
				return Value{typ: "error", str: "ERR invalid timeout value for 'PX' command"}
			}

			timeout := time.Now().Add(time.Millisecond * time.Duration(t_out))
			value.value = args[1].bulk
			value.timeout = &timeout
		}
	} else {
		value.value = args[1].bulk
		value.timeout = nil
	}

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	val, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	if val.timeout != nil && val.timeout.Before(time.Now()) {
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()

		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val.value}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()

	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}

	HSETs[hash][key] = value

	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	val, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	defer HSETsMu.RUnlock()

	val, ok := HSETs[hash]
	if !ok {
		return Value{typ: "null"}
	}

	arrVal := Value{typ: "array"}

	for _, v := range val {
		arrVal.array = append(arrVal.array, Value{typ: "bulk", bulk: v})
	}

	return arrVal
}

func keys(args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'key' command"}
	}

	SETsMu.RLock()
	defer SETsMu.RUnlock()

	arrVal := Value{typ: "array"}

	for k, _ := range SETs {
		arrVal.array = append(arrVal.array, Value{typ: "bulk", bulk: k})
	}

	return arrVal
}

func info(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'info' command"}
	}

	command := strings.ToUpper(args[0].bulk)

	switch command {
	case "REPLICATION":
		return replicationInfo()
	}

	return Value{typ: "error", str: "ERR error has occured with the 'config' command"}
}

func replicationInfo() Value {
	ConfigMu.RLock()
	defer ConfigMu.RUnlock()
	if rep := Config["replicaOf"]; rep != "" {
		return Value{typ: "bulk", bulk: "role:slave"}
	} else {
		return Value{typ: "bulk", bulk: "role:master"}
	}
}
