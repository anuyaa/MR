#!/bin/bash

source ~/.ec2/env

instances=($(ec2-describe-instances | grep running | awk {'print $2'}))
if [ ${#instances[@]} -eq 0 ] ;
then
  echo "No running instances"
  exit 0
fi
ec2-terminate-instances ${instances[@]}
