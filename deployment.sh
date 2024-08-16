if [ -f "/Users/lizhenyu/IdeaProjects/hbase/hbase-assembly/target/hbase-1.4.14-bin.tar.gz" ]; then
rm -rf /Users/lizhenyu/IdeaProjects/hbase/hbase-assembly/target/hbase-1.4.14-bin.tar.gz
fi
if [ -d "/Users/lizhenyu/IdeaProjects/hbase/hbase-assembly/target/hbase-1.4.14" ]; then
rm -rf /Users/lizhenyu/IdeaProjects/hbase/hbase-assembly/target/hbase-1.4.14
fi
mvn clean package -DskipTests assembly:single validate -Denforcer.skip=true
if [ $? -ne 0 ]; then
    echo "package fail"
    exit 1
else
    echo "package success"
fi
#mvn package assembly:single -Dmaven.main.skip=true -DskipTests
cd /Users/lizhenyu/IdeaProjects/hbase/hbase-assembly/target
if [ -d "/Users/lizhenyu/Desktop/proj_failure_recovery/hbase-1.4.14" ]; then
rm -rf /Users/lizhenyu/Desktop/proj_failure_recovery/hbase-1.4.14
fi
tar -zxvf hbase-1.4.14-bin.tar.gz
mv hbase-1.4.14 /Users/lizhenyu/Desktop/proj_failure_recovery/
echo "---------------Done---------------"