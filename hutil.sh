
function installhadoop(){
	
	if [ ! -f "$HOME/hadoop-1.0.3.tar.gz" ]; then
	    wget http://mirrors.sonic.net/apache/hadoop/common/hadoop-1.0.3/hadoop-1.0.3.tar.gz
		mv hadoop-1.0.3.tar.gz $HOME/hadoop-1.0.3.tar.gz
	fi
	tar -zxvf $HOME/hadoop-1.0.3.tar.gz -C $HOME/
	mv $HOME/hadoop-1.0.3 $HOME/hadoop
	mkdir $HOME/hadoop/hadoop_scripts
	cp hutil.sh $HOME/hadoop/hadoop_scripts/
	echo "export HADOOP_INSTALL=\$HOME/hadoop" >> ~/.bashrc
	echo "export PATH=\$PATH:\$HOME/hadoop/bin/:\$HOME/hadoop/hadoop_scripts/" >> ~/.bashrc
	source ~/.bashrc
	if [ ! -d "$HOME/.hadoop-template" ]; then
	    mkdir ~/.hadoop-template
	fi
	wget http://dl.dropbox.com/u/28320052/driver.java
	mv driver.java ~/.hadoop-template/driver.java
	wget http://dl.dropbox.com/u/28320052/reducer.java
	mv reducer.java ~/.hadoop-template/reducer.java
	wget http://dl.dropbox.com/u/28320052/mapper.java
	mv mapper.java ~/.hadoop-template/mapper.java

}

function create(){
	mkdir $1
	cd $1
	cat ~/.hadoop-template/driver.java >> $1.java
	cat ~/.hadoop-template/reducer.java >> $1Reducer.java
	cat ~/.hadoop-template/mapper.java >> $1Mapper.java
	sed -i "s/driver/$1/g" $1.java
	sed -i "s/driver/$1/g" $1Reducer.java
	sed -i "s/driver/$1/g" $1Mapper.java
	mkdir build
	mkdir build/classes
}

function compile(){
	
	driver="$1"
	mapper="$1Mapper.java"
	reducer="$1Reducer.java"
	name="$1*.java"
	javac -cp $HADOOP_INSTALL/hadoop-core-1.0.3.jar -d build/classes $name
	jar -cfe $1.jar $1 -C ./build/classes .
}

function run(){

	export HADOOP_CLASSPATH=build/classes
	hadoop jar $1.jar $2 $3
}

function uninstall(){
	rm -rf $HOME/hadoop
	sed -i "/hadoop/d" ~/.bashrc
	rm -rf ~/.hadoop-template
	source ~/.bashrc
}


function update(){

	cd $HOME
	if [ -f "hutil.sh" ]; then
	    rm -rf hutil.sh
	fi
	wget http://dl.dropbox.com/u/28320052/hutil.sh 
	chmod a+x hutil.sh
	mv hutil.sh $HADOOP_INSTALL/hadoop_scripts/ 
	wget http://dl.dropbox.com/u/28320052/driver.java
	mv driver.java ~/.hadoop-template/driver.java
	wget http://dl.dropbox.com/u/28320052/reducer.java
	mv reducer.java ~/.hadoop-template/reducer.java
	wget http://dl.dropbox.com/u/28320052/mapper.java
	mv mapper.java ~/.hadoop-template/mapper.java

}

function version(){
	echo "0.2"
}

NO_ARGS=0
function main(){
	if [ $# -eq 0 ]; then
		echo "Usage: hutil.sh command [args]"
		echo "Commands:"
		echo "install -> no args"
		echo "           install and setup hadoopppp for local use"
		echo "create  -> project/class name as argument"
		echo "		 create template for map/reduce program"
		echo "comiple -> project/class name as argument"
		echo "		 compile the java src code"
		echo "run     -> project/class name as argument"
		echo "		 run the map/reduce job"
		echo "uninstall -> no args"
		echo "		   to uninstall hadoop"
		echo "update  -> no args"
		echo "           to update the script"
		exit 65
	fi

	if [ $1 = "install" ]; then
		installhadoop
	fi

	if [ $1 = "create" ]; then
		create $2
	fi

	if [ $1 = "compile" ]; then
		compile $2
	fi

	if [ $1 = "run" ]; then
		run $2 $3 $4
	fi

	if [ $1 = "uninstall" ]; then
		uninstall
	fi

	if [ $1 = "update" ]; then
		update
	fi
	
	if [ $1 = "version" ]; then
		version
	fi
}

main $*
