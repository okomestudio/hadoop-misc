# hadoop-misc
Miscellaneous stuff for Hadoop.



Playing around with custom input format class.

    $ mkdir output
    $ javac -classpath /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.0.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.4.0.jar  -d ./output CartesianProduct.java
    $ jar cvf cartesianproduct.jar -C ./output/ .

For the original code, see:

https://github.com/adamjshook/mapreducepatterns/blob/master/MRDP/src/main/java/mrdp/ch5/CartesianProduct.java

