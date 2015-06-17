#!/bin/bash

function usage()
{
  printf "Usage:\n\ninit_cluster.sh <number_of_instances> <instance_type>\n\n"
}

function setup_instance()
{
  if [ "$#" -ne 3 ] ;
  then
    echo "${FUNCNAME}() : Type of instance (JOB_TRACKER | TASK_TRACKER) not specified"
    return
  fi
  local SSH_OPTS="-i $HOME/.ec2/id_rsa -q -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null "
  echo "Waiting for $1 to be operational"
  while ! nc -z $1 22 > /dev/null; do sleep 0.1; done
  scp $SSH_OPTS ~/.ec2/id_rsa ec2-user@$1:~/.ssh/id_rsa > /dev/null
  cat ~/.ssh/id_rsa.pub | ssh $SSH_OPTS ec2-user@$1 "cat >> ~/.ssh/authorized_keys; mkdir ~/.aws; chmod 600 ~/.ssh/id_rsa" > /dev/null
  scp $SSH_OPTS ~/.aws/credentials ec2-user@$1:~/.aws/credentials > /dev/null
  scp $SSH_OPTS ./setup_instance.sh ec2-user@$1:/tmp/setup_instance.sh > /dev/null
  ssh $SSH_OPTS ec2-user@$1 "chmod +x /tmp/setup_instance.sh && nohup /tmp/setup_instance.sh $2 '$3' > /dev/null 2>&1 &" > /dev/null
  echo ""
}

#Gives you the full directory name of the script no matter where it is being called from
export SCRIPT_HOME=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd $SCRIPT_HOME

if [ "$#" -ne 2 ] ;
then
  usage
  exit 1
fi

source ~/.ec2/env

if [ ! -r ~/.aws/credentials ] ;
then
  echo "Error: AWS SQS credentials missing at ~/.aws/credentials"
  echo "Exiting"
  exit 1
fi

ami_image="ami-1ecae776" # Amazon Linux AMI 2015.03 (HVM), SSD Volume Type
num_instances=$1
instance_type="$2"
h_line="------------------------------------------------------------------"
echo $h_line

# Get the SSH key pair
printf "Key Pair\n\n"
key_pair="$(whoami)-key-pair"
ec2-describe-keypairs | grep "$key_pair" > /dev/null
if [ $? -ne 0 ] ;
then
  if [ ! -r ~/.ec2/id_rsa.pub ] ;
  then
    echo "Error: Public key missing at ~/.ec2/id_rsa.pub"
    echo "Exiting"
    exit 1
  fi
  ec2-import-keypair $key_pair -f ~/.ec2/id_rsa.pub > /dev/null
  echo "Created new SSH key pair : $key_pair"
else
  echo "Found existing SSH key pair : $key_pair"
fi
echo $h_line

# Get security group
printf "Security Group\n\n"
sec_group_name="hashbang-security-group"
security_group=`ec2-describe-group $sec_group_name 2>/dev/null | head -n 1 | awk {'print $2'}`
if [[ "$security_group" != sg-*  ]] ;
then
  echo "Creating security group : $sec_group_name"
  security_group=`ec2-create-group $sec_group_name -d "Hashbang security group" | awk {'print $2'}`
  if [[ $security_group == sg-*  ]] ;
  then
    printf "Created group successfully.\n\n"
    ec2-authorize $sec_group_name -P icmp -t -1:-1 -s 10.0.0.0/8
    ec2-authorize $sec_group_name -p 9090-9092 -s 10.0.0.0/8
    ec2-authorize $sec_group_name -p 9999 -s 10.0.0.0/8
    ec2-authorize $sec_group_name -p 22 -s 10.0.0.0/8
    echo "Added required rules to security group"
  else
    echo "Error: Could not create security group"
    exit 1
  fi
else
  echo "Found security group : $sec_group_name"
fi
printf "Security Group ID : $security_group\n\n"

# Adding a rule for SSH in the security group from our public IP
public_ip=$(curl icanhazip.com 2>/dev/null)
ec2-describe-group $sec_group_name | grep "$public_ip/32" > /dev/null
if [ $? -ne 0 ] ;
then
  ec2-authorize $sec_group_name -p 22 -s $public_ip/32
  echo "Added SSH rule for $public_ip"
else
  echo "Found SSH rule for $public_ip"
fi
echo $h_line

echo "Creating $num_instances instance(s) in AWS EC2"
instance_ids=($(ec2-run-instances $ami_image -n $num_instances -t $instance_type -k $key_pair -g $sec_group_name | grep INSTANCE | awk {'print $2'}))
if [ ${#instance_ids[@]} -eq 0 ] ;
then
  echo "Exiting"
  exit 1
fi
echo ${instance_ids[@]}
instances=()
echo "Waiting for 30 seconds to boot up instances"
sleep 30
for i in ${instance_ids[@]}
do
  ip=$(ec2-describe-instances $i | grep running | awk {'print $13'})
  instances+=($ip)
done
echo $h_line

printf "Cluster Summary\n\n"
JOB_TRACKER=${instances[0]}
TASK_TRACKERS=${instances[@]:1}
printf "Job Tracker:\n$JOB_TRACKER\n\n"
printf "Task Trackers:\n"
echo $JOB_TRACKER > ~/.ec2/nodes
for i in $TASK_TRACKERS
do
  printf "$i\n"
  echo "$i" >> ~/.ec2/nodes
done
echo $h_line

if [[ "$instance_type" == "m3.large" ]];
then
  JAVA_OPTS="-Xms512m -Xmx6g"
elif [[ "$instance_type" == "m3.xlarge" ]];
then
  JAVA_OPTS="-Xms512m -Xmx12g"
elif [[ "$instance_type" == "c3.xlarge" ]];
then
  JAVA_OPTS="-Xms512m -Xmx6g"
elif [[ "$instance_type" == "c3.large" ]];
then
  JAVA_OPTS="-Xms512m -Xmx3g"
fi

# set up instances
setup_instance $JOB_TRACKER "JOB_TRACKER" "$JAVA_OPTS"
for i in ${TASK_TRACKERS[@]}
do
  setup_instance $i "TASK_TRACKER" "$JAVA_OPTS"
done
echo "You are all set to run the MR job on the node: $JOB_TRACKER"