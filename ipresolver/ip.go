package ipresolver

import (
    "net"
    "log"
    "os"
)

var (
    MyIP = ""
)

func GetLocalAddr() string {
	if MyIP != "" {
		return MyIP
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				MyIP = ipnet.IP.String()
			}
		}
	}
	return MyIP
}