package main

import (
	"os"
	"time"
	"net"
	"log"
	"fmt"
	"strings"
	"math/rand"
)

//utility or helper methods

//Initialize membershipList with current time and LocalIP
func initializeML(){
	node := member{currHost, time.Now().Format(time.RFC850)}
	membershipList = append(membershipList, node)
}


// returns 0 if not update, 1 if update
func updateML(hostIndex int, msg message) int {
	localTime, _ := time.Parse(time.RFC850, membershipList[hostIndex].Timestamp)
	givenTime, _ := time.Parse(time.RFC850, msg.Timestamp)

	if givenTime.After(localTime) {
		membershipList = append(membershipList[:hostIndex], membershipList[hostIndex+1:]...)
		go writeMLtoFile()
		return 1
	} else {
		//CHECK THIS LATER
		return 0
	}
}

//get index for local VM in membershipList
func getIndex() int {
	for i, element := range membershipList {
		if currHost == element.Host {
			return i
		}
	}
	return -1
}


//Logging
//Helper function to log errors
func errorCheck(err error) {
	if err != nil {
		fmt.Println("ERROR : ", err)
		errlog.Println(err)
	}
}

//Helper function to log general information
func infoCheck(info string) {
	infolog.Println(info)
}

//Helper function to log failure information
func failureCheck(fail string) {
	faillog.Println(fail)
}

//Helper function to log leaving information
func leaveCheck(leave string) {
	faillog.Println(leave)
}

//Helper function to log joining information
func joinCheck(join string) {
	faillog.Println(join)
}

//Helper function to hard reset both timers (stop both and set resetFlags to 1)
func resetTimers() {
	resetFlags[0] = 1
	resetFlags[1] = 1
	resetFlags[2] = 1
	timers[0].Reset(0)
	timers[1].Reset(0)
	timers[2].Reset(0)
}

//Sets currHost to local IP (as a string)
//Sets membershipList with currHost as its only member with current time
//Initializes timers with MAX_TIME and subsequently stops them. This is to prevent false firing of timers when Syn/Ack begins
func setupAndInitialize() {
	currHost = getIP()
	initializeML()
	timers[0] = time.NewTimer(MAX_TIME)
	timers[1] = time.NewTimer(MAX_TIME)
	timers[2] = time.NewTimer(MAX_TIME)
	timers[0].Stop()
	timers[1].Stop()
	timers[2].Stop()
	
	rand.Seed(time.Now().UTC().UnixNano())
	
	logfile_exists := 1
	if _, err := os.Stat("logfile.log"); os.IsNotExist(err) {
		logfile_exists = 0
	}
	
	logfile, _ := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	errlog = log.New(logfile, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	infolog = log.New(logfile, "INFO: ", log.Ldate|log.Ltime)
	joinlog = log.New(logfile, "JOINING: ", log.Ldate|log.Ltime)
	leavelog = log.New(logfile, "LEAVING: ", log.Ldate|log.Ltime)
	faillog = log.New(logfile, "FAILED: ", log.Ldate|log.Ltime)
	
	
	if logfile_exists == 1 {
		//TODO?
	}
}

//get local IP address in the form of a string
func getIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		errorCheck(err)
	}
	return addrs[1].String()
}

/*Takes the host for a member and checks its index with the index for the local VM. Returns 1 if
host is (localIndex + 1)%N or 2 if host is (localIndex + 2)%N, where N is size of membershipList*/
func getRelativeIndex(host string) int {
	localIndex := getIndex()
	if strings.Compare(membershipList[(localIndex+1)%len(membershipList)].Host, host) == 0 {
		return 1
	} else if strings.Compare(membershipList[(localIndex+2)%len(membershipList)].Host, host) == 0 {
		return 2
	} else if strings.Compare(membershipList[(localIndex+3)%len(membershipList)].Host, host) == 0 {
		return 3
	}	
	return -1
}