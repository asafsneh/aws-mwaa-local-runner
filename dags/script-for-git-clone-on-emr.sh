#!/bin/bash

if [[ -d ~/mobile_data_eng ]] ; then
    echo 'the repo already exists on the server'
    echo 'Exit without doing nothing!'
    exit 0
fi

if [[ $# -eq 0 ]] ; then
    echo 'You didnt mention which brnach to clone'
    echo 'Exit!'
    exit 1
fi

echo "You asked to clone git branch: $1";
sudo yum install git -y
aws s3 cp s3://ssa.bi/emr/infra/github_utils/ssh-key-for-github/id_rsa_for_github ~/.ssh/id_rsa_for_github
touch ~/.ssh/config
echo "Host github.com" >> ~/.ssh/config
echo "StrictHostKeyChecking no" >> ~/.ssh/config
sudo chmod 600 ~/.ssh/config
sudo chmod 600 ~/.ssh/id_rsa_for_github
eval `ssh-agent -s`
ssh-add ~/.ssh/id_rsa_for_github
[[ ! -d ~/mobile_data_eng ]] && git clone -b $1 git@github.com:ironsource-mobile/mobile_data_eng.git ~/mobile_data_eng