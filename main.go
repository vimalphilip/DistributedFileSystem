package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

//Declare constants
const FILE_PATH = "MembershipList.txt"   //File path of membership list
const MAX_TIME = time.Millisecond * 2500 //Max time a VM has to wait for the Syn/Ack message
const MIN_HOSTS = 4                      //Minimum number of VM's in the group before Syn/Ack begins

var INTRODUCER = "172.31.22.202/20"    //IP Address of the introducer
var currHost string                    //	IP of the local machine
var isConnected int                    //  1(Connected) or 0(Not connected) -> Boolean value to check if machine is currently connected to the group
var membershipList = make([]member, 0) //Contains all members connected to the group

//We need 2 timers to keep track of the connected nodes
var timers [3]*time.Timer

//We also need 2 flags to keep track of nodes voluntarily leaving or crashed
//1 = timers were forcefully stopped
var resetFlags [3]int

//Used if introducer crashes and reboots using a locally stored membership list
var validFlags []int

//Mutex used for membershipList and timers
var mutex = &sync.Mutex{}

//Message object passed b/w client and server
type message struct {
	Host             string
	Status           string
	Timestamp        string
	File_Information file_information
}

//Information kept for each VM in the group stored in membership list
type member struct {
	Host      string
	Timestamp string
}

//type and functions used to sort membershipLists
type memList []member

//TODO functions to sort the membershipList
func (slice memList) Len() int           { return len(slice) }
func (slice memList) Less(i, j int) bool { return slice[i].Host < slice[j].Host }
func (slice memList) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }

//Log files for error and info logging
var logfile *os.File
var errlog *log.Logger
var infolog *log.Logger
var joinlog *log.Logger
var leavelog *log.Logger
var faillog *log.Logger

//For simulating packet loss in percent
const PACKET_LOSS = 0

var packets_lost int

func main() {

	//setup and initialize starting variables
	setupAndInitialize()
	//start all the VM's to receive connections and messages from 1) Connected VM's 2) Introducer when new VM's join
	// 3) File Meta Data Server
	//1)
	go messageServer()
	//2)
	go introducerMachineServer()
	//3)
	go metaDataServer()

	//Reader to take console input from the user
	reader := bufio.NewReader(os.Stdin)

	//Check if the VM is the introducer
	if currHost == INTRODUCER {
		//If membershipList file exists, check if user wants to restart server using
		//the file or start a new group
		//If VM is the introducer, follow protocol for storing membershipList as a local file
		if _, err := os.Stat(FILE_PATH); os.IsNotExist(err) {
			writeMLtoFile()
		} else {
			fmt.Println("\nA membership list exists in the current directory.")
			fmt.Println("Would you like to restart the connection using the existing membership list? y/n\n")
			input, _ := reader.ReadString('\n')
			switch input {
			case "y\n":
				infoCheck("Restarting master...")
				fileToML() //convert the file in the directory to membershipList
				checkMLValid()
				checkValidFlags()
				writeMLtoFile()
				sendList()
			case "n\n":
				writeMLtoFile()
			default:
				fmt.Println("Invalid command")
			}
		}
	}

	// start sending sync functions and check for acks in seperate threads
	go sendSyn()
	go checkLastAck(1)
	go checkLastAck(2)
	go checkLastAck(3)

	//Take inputs from the console on what to do?
	//4 options
	// 1. Print the membershiplist
	// 2. Show this machine's IP address
	// 3. Join the group
	// 4. Leave the group

	for {
		fmt.Println("1:  Print membership list")
		fmt.Println("2:  Show IP address of this host")
		fmt.Println("3:	 Join the group")
		fmt.Println("4:	 Leave the group")
		fmt.Println("5:	 Distributed Grep\n")
		fmt.Println("6:  Add the file to DFS (<localfilename> <dfsfilename>)")
		fmt.Println("7:  Get the file (get <dfsfilename>)")
		fmt.Println("8:  Delete the file (delete <dfsfilename>)")
		fmt.Println("9:  List the files (ls <dfsfilename>)")
		fmt.Println("10: Store the file in local file system")
		input, _ := reader.ReadString('\n')
		switch input {
		case "1\n":
			if isConnected == 1 || currHost == INTRODUCER {
				for _, element := range membershipList {
					fmt.Println(element)
				}
			} else {
				fmt.Println("You are currently not connected to the group. Only nodes part of the group can see the membership list")
			}
		case "2\n":
			fmt.Println(currHost)
		case "3\n":
			if currHost != INTRODUCER {
				if isConnected == 0 {
					fmt.Println("Joining group")
					connectToIntroducer()
					joinCheck(currHost + " is connecting to introducer")
					isConnected = 1
				} else {
					fmt.Println("I am already connected to the group")
				}
			} else {
				fmt.Println("I am the introducer")
			}
		case "4\n":
			if isConnected == 1 {
				fmt.Println("Leaving group")
				leaveGroup()
				leaveCheck(currHost + " left group")
				os.Exit(0)

			} else {
				fmt.Println("You are currently not connected to a group")
			}
		case "5\n":
			fmt.Println("Enter the grep string/regular-expression. Sample syntax < -c abcd >  \n")
			input, _ := reader.ReadString('\n')
			go grepClient(input)
		case "6\n":
			fmt.Println("Enter the Local path of the File")
			local_path, _ := reader.ReadString('\n')
			local_path = strings.TrimRight(local_path, "\n")
			fmt.Println("Enter the DFS FileName:")
			dfsName, _ := reader.ReadString('\n')
			dfsName = strings.TrimRight(dfsName, "\n")
			storeFile(local_path, dfsName)
		case "7\n":
			fmt.Println("Enter the DFS FileName:")
			dfsName, _ := reader.ReadString('\n')
			dfsName = strings.TrimRight(dfsName, "\n")
			fetchFile(dfsName)	
		case "8\n":
			fmt.Println("Enter the DFS FileName:")
			dfsName, _ := reader.ReadString('\n')
			dfsName = strings.TrimRight(dfsName, "\n")
			deleteFile(dfsName)	
		case "9\n":
			fmt.Println("Enter the DFS FileName:")
			dfsName, _ := reader.ReadString('\n')
			dfsName = strings.TrimRight(dfsName, "\n")
			getFileLocations(dfsName)	
		default:
			fmt.Println("Invalid command")
		}
		fmt.Println("\n\n")
	}
}

func messageServer() {
	// We need to implement UDP to make it light weight for the heartbeat messages
	serverAddress, err := net.ResolveUDPAddr("udp", ":8010")
	errorCheck(err)
	serverConn, err := net.ListenUDP("udp", serverAddress)
	errorCheck(err)

	defer serverConn.Close()

	buf := make([]byte, 1024)

	for {
		// Constantly Listening
		msg := message{}
		n, _, err := serverConn.ReadFromUDP(buf)
		errorCheck(err)
		gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&msg)

		//Different cases
		switch msg.Status {
		case "Joining":
			node := member{msg.Host, time.Now().Format(time.RFC850)} //TODO check time format
			//todo check all conditions and if ok append to the membership list
			if checkTimeStamp(node) == 0 {
				mutex.Lock()
				resetTimers()
				membershipList = append(membershipList, node)
				sort.Sort(memList(membershipList))
				mutex.Unlock()
			}
			//check the possible conditions and if all good write to File. Also check error conditions
			go writeMLtoFile()
			sendList()
			sendFileMetaData()
		case "Leaving":
		//TODO check introducer is the node which left, if so choose a new introducer
			mutex.Lock()
			resetTimers()
			propagateMsg(msg)
			mutex.Unlock()
		case "SYN":
			infoCheck("Syn received from: " + msg.Host)
			sendAck(msg.Host)
		/*	if ack, check if ip that sent the message is either (currIndex + 1)%N or (currIndex + 2)%N or (currIndex + 2)%N
			and reset the corresponding timer to MAX_TIME*/
		case "ACK":
			if msg.Host == membershipList[(getIndex(currHost)+1)%len(membershipList)].Host {
				infoCheck("ACK received from " + msg.Host)
				timers[0].Reset(MAX_TIME)
			} else if msg.Host == membershipList[(getIndex(currHost)+2)%len(membershipList)].Host {
				infoCheck("ACK received from " + msg.Host)
				timers[1].Reset(MAX_TIME)
			} else if msg.Host == membershipList[(getIndex(currHost)+3)%len(membershipList)].Host {
				infoCheck("ACK received from " + msg.Host)
				timers[2].Reset(MAX_TIME)
			}
			//if message status is failed, propagate the message (timers will be taken care of in checkLastAck
		case "Failed":
			//resetTimers taken care in checkLastAck
			//mutex.Lock()
			if propagateMsg(msg) == 1 {
				if currHost == INTRODUCER {
					go replicate(msg.Host)
				}
			}
			INTRODUCER = membershipList[0].Host
			if currHost == INTRODUCER {
				go crossCheckMD()
			}
			//mutex.Unlock()
		case "isAlive":
			iamAlive()
			/*	received by introducer. valid flags will initially contain an array of 0's corresponding to each member
				in the membershipList. The value will be updated to 1 if an iamAlive is received from the corresponding VM */
		case "iamAlive":
			for i, element := range membershipList {
				if msg.Host == element.Host {
					validFlags[i] = 1
					break
				}
			}

		/*	received only by introducer. sent when a process wants to add a file to the sdfs. introducer first checks
			if file exists. If it does, the introducer replies with a 'file exists' message as to not overwrite data.
			Else, introducer adds file to the sdfs, adds the file to file_ips, adds the vm that sent the 'addfile' message
			to file_ips, replicates the file in the subsequent 3 processes in the membership list, and adds those 3 processes
			to the file_ips list as well.*/
		case "AddFile":
			if _, exists := file_list[msg.File_Information.FileName]; exists {
				go sendFileExists(msg)
			} else {
				ip_dest1 := membershipList[(getIndex(msg.Host)+1)%len(membershipList)].Host
				ip_dest2 := membershipList[(getIndex(msg.Host)+2)%len(membershipList)].Host
				ip_dest3 := membershipList[(getIndex(msg.Host)+3)%len(membershipList)].Host
				go sendFile(msg.Host, ip_dest1, msg.File_Information.FileName)
				go sendFile(msg.Host, ip_dest2, msg.File_Information.FileName)
				go sendFile(msg.Host, ip_dest3, msg.File_Information.FileName)

				file_ips := make([]string, 0)
				file_ips = append(file_ips, msg.Host)
				file_ips = append(file_ips, ip_dest1)
				file_ips = append(file_ips, ip_dest2)
				file_ips = append(file_ips, ip_dest3)
				info := file_information{msg.File_Information.FileName, file_ips, msg.File_Information.Size}
				file_list[msg.File_Information.FileName] = info
				sendFileMetaData()

				message := message{currHost, "FileSent", time.Now().Format(time.RFC850), info}
				var targetHosts = make([]string, 3)
				targetHosts[0] = ip_dest1
				targetHosts[1] = ip_dest2
				targetHosts[2] = ip_dest3

				sendMsg(message, targetHosts)
			}
			
		/*Received when a file was scp'ed and is the local file list needs to be updated. Checks first to see if file
		already exists in local file list. If it does, do nothing. Else, add it to the list*/	
		case "FileSent":
			exists := false
			for _, file := range local_files {
				if file == msg.File_Information.FileName {
					exists = true
					break
				}
			}
			if !exists {
				local_files = append(local_files, msg.File_Information.FileName)
				infoCheck("file " + msg.File_Information.FileName + " added to " + currHost)
			}
		case "FileExists":
			removeFile(msg.File_Information.FileName)
		/*	Received when a file was scp'ed and is the local file list needs to be updated. Checks first to see if file
			already exists in local file list. If it does, do nothing. Else, add it to the list*/		
		case "FileDoesntExist":
			fmt.Println("File does not exist")
			infoCheck("file " + msg.File_Information.FileName + " doesn't exist")			
		/*	Received only by the introducer. Checks if file exists. If it does, responds with file information. Else,
			replies with 'file does not exist' message.*/
		case "getFileLocations":
			Message := message{"", "", "", file_information{"", nil, 0}}
			if tgtFileInfo, exists := file_list[msg.File_Information.FileName]; exists {
				Message = message{currHost, "sentFileLocations", time.Now().Format(time.RFC850), tgtFileInfo}
			} else {
				Message = message{currHost, "FileDoesntExist", time.Now().Format(time.RFC850), file_information{msg.File_Information.FileName, nil, 0}}
			}
			var targetHosts = make([]string, 1)
			targetHosts[0] = msg.Host
			sendMsg(Message, targetHosts)
		/*	Received by the host that requested file locations if the file exists*/
		case "sentFileLocations":
			for _, element := range msg.File_Information.ReplicatedIPs {
				fmt.Println(element)
			}
		/*	If processes that receives this message is introducer, check to see if file exists. If it does delete file
			from file_list and send a 'DeleteFile' message to all machines that contain file. Else, reply with a
			'file does not exist' message. If process that receives message is not introducer, delete the file*/
		case "DeleteFile":
			if currHost == INTRODUCER {
				if tgtFileInfo, exists := file_list[msg.File_Information.FileName]; exists {
					delete(file_list, msg.File_Information.FileName)
					sendFileMetaData()
					sendDeleteFile(tgtFileInfo)
				} else {
					message := message{currHost, "FileDoesntExist", time.Now().Format(time.RFC850), file_information{msg.File_Information.FileName, nil, 0}}
					var targetHosts = make([]string, 1)
					targetHosts[0] = msg.Host
					sendMsg(message, targetHosts)
				}
			} else {
				removeFile(msg.File_Information.FileName)
			}
		/*	Received only by the introducer. Checks if file exists: If it does, scp's file from one of the
			machines that contains the file to the machine that requested the file. Else, replies with a file
			does not exist message.*/
		case "requestFile":
			fmt.Println("File requested")
			if tgtFileInfo, exists := file_list[msg.File_Information.FileName]; exists {
				//Does not take into account if the first ip in tgtFileInfo's IP's list fails during transfer
				go sendFile(tgtFileInfo.ReplicatedIPs[0], msg.Host, msg.File_Information.FileName)
				new_file_ips := tgtFileInfo.ReplicatedIPs
				new_file_ips = append(new_file_ips, msg.Host)
				info := file_information{msg.File_Information.FileName, new_file_ips, tgtFileInfo.Size}
				file_list[msg.File_Information.FileName] = info
				message := message{currHost, "FileSent", time.Now().Format(time.RFC850), tgtFileInfo}
				var targetHosts = make([]string, 1)
				targetHosts[0] = msg.Host

				sendMsg(message, targetHosts)
			} else {
				message := message{currHost, "FileDoesntExist", time.Now().Format(time.RFC850), file_information{msg.File_Information.FileName, nil, 0}}
				var targetHosts = make([]string, 1)
				targetHosts[0] = msg.Host
				sendMsg(message, targetHosts)
			}		
		}
		
	}

}

func introducerMachineServer() {
	//Listens to messages from introducer

	serverAddress, err := net.ResolveUDPAddr("udp", ":8011")
	errorCheck(err)
	serverConn, err := net.ListenUDP("udp", serverAddress)
	errorCheck(err)

	defer serverConn.Close()

	buf := make([]byte, 1024)

	for {
		// Constantly Listening
		mList := make([]member, 0)
		n, _, err := serverConn.ReadFromUDP(buf)
		err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&mList)
		errorCheck(err)

		//restart timers if membershipList is updated
		mutex.Lock()
		resetTimers()
		membershipList = mList
		mutex.Unlock()

		var msg = "New VM joined the group: \n\t["
		var size = len(mList) - 1
		for i, host := range mList {
			msg += "(" + host.Host + " | " + host.Timestamp + ")"
			if i != size {
				msg += ", \n\t"
			} else {
				msg += "]"
			}
		}
		infoCheck(msg)
	}
}

//VM's are marked as failed if they have not responded with an ACK within MAX_TIME
//2 checkLastAck calls persist at any given time, one to check the VM at (currIndex + 1)%N and one to
//check the VM (currIndex + 2)%N, where N is the size of the membershipList
//relativeIndex can be 1 or 2 and indicates what VM the function to watch
//A timer for each of the two VM counts down from MAX_TIME and is reset whenever an ACK is received (handled in
// messageServer function.
//Timers are reset whenever the membershipList is modified
//The timer will reach 0 if an ACK isn't received from the corresponding VM
// within MAX_TIME, or the timer is reset. If a timer was reset, the corresponding resetFlag will be 1
// and indicate that checkLastAck should be called again and that the failure detection should not be called
//If a timer reaches 0 because an ACK was not received in time, the VM is marked as failed and the message is
//propagated to the next 2 VM's in the membershipList. Both timers are then restarted.
func checkLastAck(relativeIndex int) {
	//Wait until number of members in group is at least MIN_HOSTS before checking for ACKs
	for len(membershipList) < MIN_HOSTS {
		time.Sleep(100 * time.Millisecond)
	}

	//Get host at (currIndex + relativeIndex)%N
	host := membershipList[(getIndex(currHost)+relativeIndex)%len(membershipList)].Host
	infoCheck("Checking " + string(relativeIndex) + ": " + host)

	//Create a new timer and hold until timer reaches 0 or is reset
	timers[relativeIndex-1] = time.NewTimer(MAX_TIME)
	<-timers[relativeIndex-1].C

	/*	3 conditions will prevent failure detection from going off
		1. Number of members is less than the MIN_HOSTS
		2. The target host's relative index is no longer the same as when the checkLastAck function was called. Meaning
		the membershipList has been updated and the checkLastAck should update it's host
		3. resetFlags for the corresponding timer is set to 1, again meaning that the membership list was updated and
		checkLastack needs to reset the VM it is monitoring.*/
	mutex.Lock()
	if len(membershipList) >= MIN_HOSTS && getRelativeIndex(host) == relativeIndex && resetFlags[relativeIndex-1] != 1 {
		msg := message{membershipList[(getIndex(currHost)+relativeIndex)%len(membershipList)].Host, "Failed", time.Now().Format(time.RFC850), file_information{"", nil, 0}}
		fmt.Print("Failure detected: ")
		fmt.Println(msg.Host)
		failureCheck("Failure detected: " + msg.Host)
		propagateMsg(msg)

	}
	//If a failure is detected for one timer, reset the other as well.
	if resetFlags[relativeIndex-1] == 0 {
		infoCheck("Force stopping timer " + string(relativeIndex))
		resetFlags[relativeIndex%2] = 1
		timers[relativeIndex%2].Reset(0)
		resetFlags[relativeIndex%3] = 1
		timers[relativeIndex%3].Reset(0)
	} else {
		resetFlags[relativeIndex-1] = 0
	}

	mutex.Unlock()
	go checkLastAck(relativeIndex)

}
