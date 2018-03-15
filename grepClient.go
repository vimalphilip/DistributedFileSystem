// grep_client
package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"
)

func grepClient(serverInput string) {
	const PORT = "8008"
	ipList := []string{}

	//Compile list of ip address from masterlist.txt
	for index, element := range membershipList {
		infoCheck("Grep from "+element.Host)
		ip_content, _, _ := net.ParseCIDR(membershipList[index].Host)
		var ip_add = ip_content.String() +":"+ PORT
		ipList = append(ipList, ip_add)
	}

	t0 := time.Now()

		c := make(chan string)
		
		// Connect to every server in masterlist.txt
		for i := 0; i < len(ipList); i++ {
			go writeToServer(ipList[i], serverInput, c)
		}
		// Print results from server and write to a file
		_, err := os.Stat("logGrep")
		if	os.IsNotExist(err) {
		  _, err := os.Create("logGrep")
		  if err != nil {
	        panic(err)
		  }
		} 
		f, err := os.OpenFile("logGrep", os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
				panic(err)
				}
	    defer f.Close()
    
		for i := 0; i < len(ipList); i++ {
			serverResult := <-c
			fmt.Println(serverResult)
			fmt.Println("----------")
			_, err = f.WriteString(serverResult)
		}
		f.Sync()
		w := bufio.NewWriter(f)
		w.Flush()
	

	t1 := time.Now()
	fmt.Print("Function took: ")
	fmt.Println(t1.Sub(t0))
}


/*
 * Sends a message to a server, and returns the file into a channel
 * @param ipAddr string representation of the server's IP Address
 * @param message the message to be sent back to the server
 * @param c the channel for returning server messages
 */
func writeToServer(ipAddr string, message string, c chan string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", ipAddr)
	if err != nil {
		c <- err.Error()
		return
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		c <- err.Error()
		return
	}

	_, err = conn.Write([]byte(message))
	if err != nil {
		c <- err.Error()
		return
	}

	result, err := ioutil.ReadAll(conn)
	if err != nil {
		c <- err.Error()
		return
	}

	c <- string(result)
}
