# Implementation for VEIL: A Storage and Communication Efficient Volume-Hiding Algorithm

To run the code:
1. download the TPC-H skew benchmark: https://github.com/YSU-Data-Lab/TPC-H-Skew 
2. generate TPC-H skew dataset using the benchmark (skew factor = 0.4): ./dbgen -s 1 -z 0.4 -T L
3. process the generated data file so that it is suitable for postgresql. The sample code is in exp_data_processing/file_operation.py
4. install postgresql
5. create a database "tpch_6mz0_4" in postgresql: create database tpch_6mz0_4
6. create a lineitem table in database tpch_6mz0_4 in postgresql: 
  CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(50) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL);
7. inject data into the table: COPY lineitem FROM '/lineitem.tbl' ( FORMAT CSV, DELIMITER('|') );
8. run the java code: 
   1. Disjoint approach: src/test/java/experiments/Test_MultiMap_Disjoint.java
   2. Overlapping approach: src/test/java/experiments/Test_MultiMap_Overlapping.java
   3. Overlapping approach with user-desired overlapping size: src/test/java/experiments/Test_MultiMap_Overlapping_with_User_Defined_Overlap.java
   4. dprfMM: src/test/java/experiments/Test_DprfMM.java
   5. XORMM: src/test/java/experiments/Test_XorMM.java
