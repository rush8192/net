sudo apt-get update
sudo apt-get install -y mercurial gcc libc6-dev
hg clone -u default https://code.google.com/p/go $HOME/go
cd $HOME/go/src
./all.bash
#add to .profile
#export PATH=$PATH:$HOME/go/bin
#export GOPATH=$HOME/cs/net