DATE=`date "+%Y_%m_%d_%H_%M_%S"`
DIR_NAME=${PWD##*/}
TAR_NAME=$DIR_NAME.$DATE.tar.gz
echo $TAR_NAME
make clean
rm -rf datafile*
cd ..; tar czvf $TAR_NAME ./$DIR_NAME
echo "Done"
