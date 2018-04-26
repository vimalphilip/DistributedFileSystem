package main

import (
		"time"
		"fmt"
		"os/exec"
		"os"
		"net"
		"bytes"
		"encoding/gob"
		"strings"
)

// Make a structure to store file info: name, VMs it's replicated on, and size
type file_information struct {
	FileName string
	ReplicatedIPs []string
	Size int64
}

/*Stores sdfs names for all files stored locally*/
var local_files = make([]string, 0)

/*dictionary with keys = filenames and values = array of ip's corresponding to machines
the file is replicated on. Only maintined in introducer and following 2 machines in membershiplist*/
var file_list = make(map[string]file_information)

// Write a function to store file
/* Stores the file in DFS   
Adds file to DFS given a local path and requested DFS name. First checks if file already exists locally
in DFS file directoy. If it does, it returns. Copies file from local path to DFS file directory and sends
an add file message to introducer to request to add file if the machine is not the introducer. If the machine
is the introducer, it scp's the file to the next 3 machines to replicate the file and updates the file list
*/

func storeFile(localPath string, dfsName string) {
	//first check if file exists
	if check_if_exists(dfsName) == 1 {
		return
	}
	_, err := exec.Command("cp", localPath, "/home/ec2-user/distributedFileSystem/files/"+dfsName).Output()
	errorCheck(err)
	if err != nil {
		fmt.Println("Local file does not exist")
		return
	}
	//checks if the host is the introducer
	if getIP() != INTRODUCER {
		sendAddFile(dfsName, getFileSize(localPath))
	} else {
		//TODO What if file exists?
		ip_dest1 := membershipList[(getIndex(currHost)+1)%len(membershipList)].Host
		ip_dest2 := membershipList[(getIndex(currHost)+2)%len(membershipList)].Host
		ip_dest3 := membershipList[(getIndex(currHost)+3)%len(membershipList)].Host
		go sendFile(currHost, ip_dest1, dfsName)
		go sendFile(currHost, ip_dest2, dfsName)
		go sendFile(currHost, ip_dest3, dfsName)

		file_ips := make([]string, 0)
		file_ips = append(file_ips, currHost)
		file_ips = append(file_ips, ip_dest1)
		file_ips = append(file_ips, ip_dest2)
		file_ips = append(file_ips, ip_dest3)
		file_list[dfsName] = file_information{dfsName, file_ips, getFileSize(localPath)}
		//sendFileMD()

		message := message{currHost, "FileSent", time.Now().Format(time.RFC850), file_list[dfsName]}
		var targetHosts = make([]string, 3)
		targetHosts[0] = ip_dest1
		targetHosts[1] = ip_dest2
		targetHosts[2] = ip_dest3

		sendMsg(message, targetHosts)
	}
	local_files = append(local_files, dfsName)
	infoCheck("file " + dfsName + " added to "+ currHost)
	
	
}

/*Function to delete file from the DFS. If the machine is the introducer, it deletes the file by calling
sendDeleteFile function. Else, the machine sends a 'deletefile' request to the introducer*/
func deleteFile(dfsName string) {
	if currHost != INTRODUCER {
		msg := message{currHost, "DeleteFile", time.Now().Format(time.RFC850), file_information{dfsName, nil, 0}}
		var targetHosts = make([]string, 1)
		targetHosts[0] = INTRODUCER
		sendMsg(msg, targetHosts)
	} else {
		if tgtFileInfo, exists := file_list[dfsName]; exists {
			sendDeleteFile(tgtFileInfo)
		} else {
			fmt.Println("File does not exist")
		}
	}
}

//fetch the file and store into local directory
/*Function to pull a file. If the machine is the introducer, it checks file_list to get an ip of a machine the file
is replicated on, scps the file, and updates its local file list. Else, the machine sends a 'requestFile' message
to the introducer*/
func fetchFile(dfsName string) {
	if check_if_exists(dfsName) == 1 {
		fmt.Println("File already exists in local 'files' directory")
		return
	}
	if currHost != INTRODUCER {
		msg := message{currHost, "requestFile", time.Now().Format(time.RFC850), file_information{dfsName, nil, 0}}
		var targetHosts = make([]string, 1)
		targetHosts[0] = INTRODUCER
		sendMsg(msg, targetHosts)
	} else {
		if tgtFileInfo, exists := file_list[dfsName]; exists {
			//Does not take into account if the first ip in tgtFileInfo's IP's list fails during transfer
			go sendFile(tgtFileInfo.ReplicatedIPs[0], currHost, dfsName)
			local_files = append(local_files, dfsName)
			infoCheck("file " + dfsName + " fetched")
		} else {
			fmt.Println("File does not exist")
		}
	}
}

/*Check metadata with membershiplist to ensure correctness*/
func crossCheckMD() {
	for key, value := range file_list { //key = filename; value = filestruct
		for index, ip := range value.ReplicatedIPs { //itererate over ips
			exist := false
			for _, m := range membershipList {
				if m.Host == ip {
					exist = true
					break
				}
			}
			if !exist {
				new_file_ips := append(value.ReplicatedIPs[0:index], value.ReplicatedIPs[index+1:]...)
				info := file_information{key, new_file_ips, file_list[key].Size}
				file_list[key] = info
				//Only replicate file again if there are < min number of replicas
				if len(value.ReplicatedIPs) <= 4 {
					tgtIP := membershipList[(getIndex(value.ReplicatedIPs[len(value.ReplicatedIPs)-1])+1)%len(membershipList)].Host
					for _, srcIP := range value.ReplicatedIPs {
						if srcIP != ip {
							message := message{currHost, "FileSent", time.Now().Format(time.RFC850), value}
							var targetHosts = make([]string, 1)
							targetHosts[0] = tgtIP
							sendMsg(message, targetHosts)

							sendFile(srcIP, tgtIP, key)
						}
					}
					new_file_ips = append(file_list[key].ReplicatedIPs, tgtIP)
					info = file_information{key, new_file_ips, file_list[key].Size}
					file_list[key] = info
				}
			}
		}
	}

	sendFileMetaData()
}




/*Sends 'deletefile' message to all ips that have replicated the targetfile*/
func sendDeleteFile(tgtFileInfo file_information) {
	fmt.Println("Deleting file")
	for _, machine := range membershipList {
		if machine.Host != INTRODUCER {
			msg := message{currHost, "DeleteFile", time.Now().Format(time.RFC850), tgtFileInfo}
			var targetHosts = make([]string, 1)
			targetHosts[0] = machine.Host
			sendMsg(msg, targetHosts)
		} else {
			removeFile(tgtFileInfo.FileName) 
		}
	}
}



//Utility Function
/*helper function to check if file exists locally in the sdfs file directory*/
func check_if_exists(dfsName string) int {
	for _, element := range local_files {
		if element == dfsName {
			fmt.Println("File with name " + dfsName + " exists in sdfs")
			return 1
		}
	}
	return 0
}


/*helper function to remove file from local sdfs file directory and update local_files list*/
func removeFile(dfsName string) {
	_, err := exec.Command("rm", "/home/ec2-user/distributedFileSystem/files/"+dfsName).Output()
	errorCheck(err)

	for index, element := range local_files {
		if element == dfsName {
			local_files = append(local_files[0:index], local_files[index+1:]...)
			break
		}
	}
	infoCheck("file " + dfsName + " removed from " + currHost)
}

/*Helper function for sending an 'addFile' message*/
/*The host(will not be the introducer) will add itself and next 2 neighbours to the list which shows the hosts 
which replicate the file. After this, the introducer has to be informed about this and introducer 
updates it's metadata*/
func sendAddFile(dfsName string, file_size int64) {
	file_ips := make([]string, 0)
	file_ips = append(file_ips, getIP())
	curr_index := getIndex(currHost)
	file_ips = append(file_ips, membershipList[(curr_index+1)%len(membershipList)].Host)
	file_ips = append(file_ips, membershipList[(curr_index+2)%len(membershipList)].Host)
	msg := message{currHost, "AddFile", time.Now().Format(time.RFC850), file_information{dfsName, file_ips, file_size}}
	var targetHosts = make([]string, 1)
	targetHosts[0] = INTRODUCER

	sendMsg(msg, targetHosts)
}

/*helper function to get file size*/
func getFileSize(local_path string) int64 {
	file, err := os.Open(local_path)
	if err != nil {
		errorCheck(err)
		return -1
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		errorCheck(err)
		return -1
	}
	return stat.Size()
}

/*Function to scp file given the source machine, destination machine, and sdfs name*/
func sendFile(ip_src string, ip_dest string, dfsName string) {
	IP_src, _, _ := net.ParseCIDR(ip_src)
	IP_dest, _, _ := net.ParseCIDR(ip_dest)
	infoCheck("scp" +"-i /home/ec2-user/dGrep/distributed9.pem"+ "-3"+ " ec2-user@" + IP_src.String() + ":/home/ec2-user/distributedFileSystem/files/" + dfsName + " ec2-user@" + IP_dest.String() + ":/home/ec2/distributedFileSystem/files")
	cmd := exec.Command("scp", "-i /home/ec2-user/dGrep/distributed9.pem","-o StrictHostKeyChecking=no", "-o UserKnownHostsFile=/dev/null", "-3", "ec2-user@"+IP_src.String()+":/home/ec2-user/distributedFileSystem/files/"+dfsName, "ec2-user@"+IP_dest.String()+":/home/ec2-user/distributedFileSystem/files")
	fmt.Println("Command executed: "+ strings.Join(cmd.Args, " "))
	output, err :=cmd.CombinedOutput()
	if err != nil {
    fmt.Println(fmt.Sprint(err) + ": " + string(output))
	}
	errorCheck(err)
}

/*Function to set up the message to replicate file_list from introducer to the next 3 machines in membershiplist*/
func sendFileMetaData() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(file_list); err != nil {
		errorCheck(err)
	}

	localip, _, _ := net.ParseCIDR(currHost)
	LocalAddr, err := net.ResolveUDPAddr("udp", localip.String()+":0")
	errorCheck(err)

	for i := 0; i < len(membershipList); i++ {
		if membershipList[i].Host != INTRODUCER {
			ip, _, _ := net.ParseCIDR(membershipList[i].Host)

			ServerAddr, err := net.ResolveUDPAddr("udp", ip.String()+":8012")
			errorCheck(err)

			conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
			errorCheck(err)

			_, err = conn.Write(buf.Bytes())
			errorCheck(err)
		}
	}
}

/*server to receive file_list udpates*/
func metaDataServer() {
	ServerAddr, err := net.ResolveUDPAddr("udp", ":8012")
	errorCheck(err)

	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	errorCheck(err)
	defer ServerConn.Close()

	buf := make([]byte, 1024)

	for {
		fileList := make(map[string]file_information)
		n, _, err := ServerConn.ReadFromUDP(buf)
		err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&fileList)
		file_list = fileList
		infoCheck("Metadata replicated at " + currHost)
		errorCheck(err)
	}
}

/*Check metadata with membershiplist to ensure correctness*/
func crossCheckMetaData() {
	for key, value := range file_list { //key = filename; value = filestruct
		for index, ip := range value.ReplicatedIPs { //itererate over ips
			exist := false
			for _, m := range membershipList {
				if m.Host == ip {
					exist = true
					break
				}
			}
			if !exist {
				new_file_ips := append(value.ReplicatedIPs[0:index], value.ReplicatedIPs[index+1:]...)
				info := file_information{key, new_file_ips, file_list[key].Size}
				file_list[key] = info
				//Only replicate file again if there are < min number of replicas
				if len(value.ReplicatedIPs) <= 4 {
					tgtIP := membershipList[(getIndex(value.ReplicatedIPs[len(value.ReplicatedIPs)-1])+1)%len(membershipList)].Host
					for _, srcIP := range value.ReplicatedIPs{
						if srcIP != ip {
							message := message{currHost, "FileSent", time.Now().Format(time.RFC850), value}
							var targetHosts = make([]string, 1)
							targetHosts[0] = tgtIP
							sendMsg(message, targetHosts)

							sendFile(srcIP, tgtIP, key)
						}
					}
					new_file_ips = append(file_list[key].ReplicatedIPs, tgtIP)
					info = file_information{key, new_file_ips, file_list[key].Size}
					file_list[key] = info
				}
			}
		}
	}

	sendFileMetaData()
}

/*Function to replicate a file. Called when a machine fails and the files it contained need to be
replicated on an additional machine*/
func replicate(host string) {
	for key, value := range file_list { //key = filename; value = filestruct
		for index, ip := range value.ReplicatedIPs { //itererate over ips
			if ip == host {
				new_file_ips := append(value.ReplicatedIPs[0:index], value.ReplicatedIPs[index+1:]...)
				info := file_information{key, new_file_ips, file_list[key].Size}
				file_list[key] = info
				//Only replicate file again if there are < min number of replicas
				if len(value.ReplicatedIPs) <= 4 {
					fmt.Println("Replication called")
					tgtIP := membershipList[(getIndex(value.ReplicatedIPs[len(value.ReplicatedIPs)-1])+1)%len(membershipList)].Host
					for _, srcIP := range value.ReplicatedIPs {
						if srcIP != host {
							fmt.Println("value: " + value.ReplicatedIPs[len(value.ReplicatedIPs)-1])
							message := message{currHost, "FileSent", time.Now().Format(time.RFC850), value}
							var targetHosts = make([]string, 1)
							targetHosts[0] = tgtIP
							sendMsg(message, targetHosts)

							sendFile(srcIP, tgtIP, key)
						}
					}
					new_file_ips = append(file_list[key].ReplicatedIPs, tgtIP)
					info = file_information{key, new_file_ips, file_list[key].Size}
					file_list[key] = info
				}
			}

		}
	}
	sendFileMetaData()

}

/*Helper function for sending a 'fileExists' message*/
func sendFileExists(msg message) {
	msg = message{getIP(), "FileExists", time.Now().Format(time.RFC850), msg.File_Information}
	var targetHosts = make([]string, 1)
	targetHosts[0] = msg.Host

	sendMsg(msg, targetHosts)
}

/*Function to print the ips of all machines in which the file is replicated given its dfs name. If the machine
is the introducer, it checks its file_list and prints the ips. Else, the machine sends a 'getFileLocations'
message to the introducer*/
func getFileLocations(dfsName string) {
	if currHost != INTRODUCER {
		msg := message{currHost, "getFileLocations", time.Now().Format(time.RFC850), file_information{dfsName, nil, 0}}
		var targetHosts = make([]string, 1)
		targetHosts[0] = INTRODUCER
		sendMsg(msg, targetHosts)
	} else {
		for _, element := range file_list[dfsName].ReplicatedIPs {
			fmt.Println(element)
		}
	}

}

