package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"time"
	"math/rand"
)

/*
 Handles connection protocol and writes message to server
 Takes a message and the IP's of the VM's to send the message as a slice of strings
 Messages can be encoded using golang's gobbing protocol

*/

func sendMsg(msg message, targetHosts []string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		errorCheck(err)
	}

	localIP, _, _ := net.ParseCIDR(currHost)
	localAddr, err := net.ResolveUDPAddr("udp", localIP.String()+":0")
	errorCheck(err)

	for _, host := range targetHosts {
		if msg.Status == "Leaving" || msg.Status == "Failed" {
			fmt.Println("Propogating ", msg, " to :", host)
		}
		ip, _, _ := net.ParseCIDR(host)

		serverAddr, err := net.ResolveUDPAddr("udp", ip.String()+":8010")
		errorCheck(err)
		conn, err := net.DialUDP("udp", localAddr, serverAddr)
		errorCheck(err)

		randNum := rand.Intn(100)
		infoCheck("Random number = " + string(randNum))
		
		//TO SIMULATE PACKET LOSS CHANGE THE 'PACKET_LOSS' constant from 0 to a valid +ve number

		if !((msg.Status == "SYN" || msg.Status == "ACK" || msg.Status == "Failed" || msg.Status == "Leaving") && randNum < PACKET_LOSS) {
			_, err = conn.Write(buf.Bytes())
			errorCheck(err)
		} else {
			packets_lost++
			fmt.Print(packets_lost," Message(s) could not be sent because of packet loss: ",msg)
		}
	}

}

//VM's ping the next 3 members in the membershipList for an ACK
func sendSyn() {
	for {
		N := len(membershipList)
		if N >= MIN_HOSTS {
			msg := message{getIP(), "SYN", time.Now().Format(time.RFC850)}
			var targetHosts = make([]string, 3)
			targetHosts[0] = membershipList[(getIndex()+1)%len(membershipList)].Host
			targetHosts[1] = membershipList[(getIndex()+2)%len(membershipList)].Host
			targetHosts[2] = membershipList[(getIndex()+3)%len(membershipList)].Host

			sendMsg(msg, targetHosts)
		}
		time.Sleep(1 * time.Second)
	}
}

//Called when a VM receives a SYN. An ACK is sent back to the VM which sent the SYN
func sendAck(host string) {
	msg := message{currHost, "ACK", time.Now().Format(time.RFC850)}
	var targetHosts = make([]string, 1)
	targetHosts[0] = host

	sendMsg(msg, targetHosts)
}

//Message sent to introducer from a VM to connect to the group
func connectToIntroducer() {
	msg := message{currHost, "Joining", time.Now().Format(time.RFC850)}
	fmt.Println("Message transfered: ", msg)
	var targetHosts = make([]string, 1)
	targetHosts[0] = INTRODUCER

	sendMsg(msg, targetHosts)

}

//Leave group
//Message sent to previous 3 VM's in membershiplist notifying that the VM is leaving the group
func leaveGroup() {
	msg := message{currHost, "Leaving", time.Now().Format(time.RFC850)}

	var targetHosts = make([]string, 3) //make a string array of size 3 to send to previous 3 VM's
	for i := 1; i < 4; i++ {
		var targetHostIndex = (getIndex() - i) % len(membershipList)
		if targetHostIndex < 0 {
			targetHostIndex = len(membershipList) + targetHostIndex
		}
		targetHosts[i-1] = membershipList[targetHostIndex].Host
	}

	sendMsg(msg, targetHosts)

}

/* Called when messages need to be propogated to the rest of the VM's.
Example: when member leaves or fails.
Messages are propogated to the next two members in the membershipList
If a member is not in the local membershipList then the message is ignored i.e if
a member Ip address is to be removed and its already not present it means that its already removed and
change has been made.
If member is there in the membershipList, call updateML to compare the timestamps and updates the membershipList
The message is then propogated to the next 3 VM's in the membershipList
*/
func propagateMsg(msg message) {
	var hostIndex = -1
	for i, element := range membershipList {
		if msg.Host == element.Host {
			hostIndex = i
			break
		}
	}
	if hostIndex == -1 { // case where node is already removed and hence could not find the node in the Mlist
		return
	}

	// msgCheck(msg)
	updateML(hostIndex, msg)

	var targetHosts = make([]string, 3)
	targetHosts[0] = membershipList[(getIndex()+1)%len(membershipList)].Host
	targetHosts[1] = membershipList[(getIndex()+2)%len(membershipList)].Host
	targetHosts[2] = membershipList[(getIndex()+3)%len(membershipList)].Host

	sendMsg(msg, targetHosts)

}

//Called by the introducer if a new member joins the group
func sendList() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(membershipList); err != nil {
		errorCheck(err)
	}
	for index, element := range membershipList {
		if element.Host != currHost {
			ip, _, _ := net.ParseCIDR(membershipList[index].Host)

			ServerAddr, err := net.ResolveUDPAddr("udp", ip.String()+":8011")
			errorCheck(err)

			localip, _, _ := net.ParseCIDR(currHost)
			LocalAddr, err := net.ResolveUDPAddr("udp", localip.String()+":0")
			errorCheck(err)

			conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
			errorCheck(err)

			_, err = conn.Write(buf.Bytes())
			errorCheck(err)
		}
	}
}

//Response from VM's to the introducer in response to isAlive. Sent to indicate to the INTRODUCER
//that the VM is still connected to the group so the INRODUCER doesn't delete it from its membershiplist
func iamAlive() {
	msg := message{currHost, "iamAlive", time.Now().Format(time.RFC850)}
	var targetHosts = make([]string, 1)
	targetHosts[0] = INTRODUCER

	sendMsg(msg, targetHosts)

}
