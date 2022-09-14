#Below are the commands which needs to be executed on the app server for the app to function
sudo apt-get update -y
sudo apt install default-jre -y
sudo apt install scala -y
sudo apt install python3-pip -y
sudo apt install awscli -y



#need to add creation of inout and outout folders
cd /home/ubuntu
mkdir input
mkdir output
mkdir output_part_files
