#!/bin/bash

USER="ec2-user"
HOME="/home/$USER"
TMP_DIR="/tmp/hashbang/mr"
JAR_DIR="/tmp/hashbang/jar"

# set up env variables
cat << EOF > $HOME/.hashbang
export HASHBANG_USER=$USER
export HASHBANG_HOME=$HOME
export HASHBANG_TMP_DIR=$TMP_DIR
export HASHBANG_JAR_DIR=$JAR_DIR
export PATH=$PATH:$HOME/local/bin
EOF

# add .hashbang to .bashrc
echo "source $HOME/.hashbang" >> $HOME/.bashrc

mkdir -p $TMP_DIR
mkdir -p $JAR_DIR

mkdir -p $HOME/local/bin
cat << EOF > $HOME/local/bin/stop_all
#!/bin/bash

killall java
rm -rf $HOME/* $TMP_DIR/*

EOF
chmod +x $HOME/local/bin/stop_all

. $HOME/.hashbang

echo "StrictHostKeyChecking   no" > $HASHBANG_HOME/.ssh/config
chmod 600 $HOME/.ssh/config

JAR_FILE="$HOME/mr_framework.jar"

S3_JAR_URL="https://s3.amazonaws.com/hashbang-mr/mr_framework.jar"
wget $S3_JAR_URL

JAVA_OPTS="$2"

if [ "$1" == "JOB_TRACKER" ] ;
then
  nohup java -d64 -cp $JAR_FILE org.hashbang.jobtracker.JobTracker > ~/job_tracker.log 2>&1 &
  nohup java -d64 $JAVA_OPTS -cp $JAR_FILE org.hashbang.fs.NamenodeService > ~/namenode.log 2>&1 &
elif [ "$1" == "TASK_TRACKER" ] ;
then
  nohup java -d64 $JAVA_OPTS -cp $JAR_FILE org.hashbang.tasktracker.TaskTracker > ~/task_tracker.log 2>&1 &
fi