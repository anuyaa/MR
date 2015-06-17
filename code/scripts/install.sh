#!/bin/bash

#Gives you the full directory name of the script no matter where it is being called from
export SCRIPT_HOME=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd $SCRIPT_HOME

wget http://s3.amazonaws.com/ec2-downloads/ec2-api-tools.zip -O /tmp/ec2-api-tools.zip
test -r ~/local/ec2 && rm -rf ~/local/ec2
mkdir -p ~/local/ec2
unzip /tmp/ec2-api-tools.zip -d ~/local/ec2
rm -f /tmp/ec2-api-tools.zip

rm -rf ~/.ec2
mkdir ~/.ec2 2>/dev/null
ssh-keygen -t rsa -P "" -f ~/.ec2/id_rsa > /dev/null

EC2_HOME=$HOME/local/ec2/$(ls $HOME/local/ec2/)
echo "export EC2_HOME=$EC2_HOME" > ~/.ec2/env
echo "export PATH=$PATH:$EC2_HOME/bin" >> ~/.ec2/env
echo "source ~/.ec2/env" >> ~/.bashrc

cp $SCRIPT_HOME/hashbang_job $EC2_HOME/bin/
chmod +x $EC2_HOME/bin/hashbang_job

source ~/.ec2/env
ec2-delete-keypair $USER-key-pair > /dev/null
echo "Set up AWS credentials as env variables" 
