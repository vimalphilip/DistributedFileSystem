The DistributedFileSystem is built on top of the GrepService and GroupMembershipService.
Please refer to the ReportDocument to find architectural design and more details about the system.

Steps to follow.
Note: Links to distributedGrep, distributedGroupMembership can be found in my profile. 

Step 1: Import all the files into 4 or more servers (Developed and tested this code on amazon EC2 instances and docker)
Step 2: Import files from distributedGrep into the same servers under the foldername "dGrep"
Step 3: Open setup.config in dGrep and change the IP addresses of the instances
Step 4: Run the command "sh setup.sh setup.config" only on one machine. This will run server.go to receive grep requests remotely
Step 5: Goto distributedFileSystem folder and execute the command "go run main.go grepClient.go distributedFileSystem.go messages.go utilities.go introducer_reboot.go". Run this command in all the instances.
Step 6: Make the instances join the group by selecting option 3 to be made part of the group and start with distributedFileSystem
Step 7: Choose from the list of options to add, delete and store the file locally. DFS files will be stored in "files" folder


Important points to take care based on the distributed system you are using
---------------------------------------------------------------------------
Amazon EC2
1. You need the .pem key file to SSH or perform SCP into other remote servers
2. Put the list of IP's and their .pem files in the config file under ~/.ssh. If there is no config file, create one. Otherwise there will be "public key (access denied)" issue.
3. chmod 400 the .pem file to set permission correctly
4. chmod 600 config file to set its permission correctly




