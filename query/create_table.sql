------------1.建表
CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));

CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL );

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL );

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);

CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                           O_CLERK          CHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);

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
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL);





------------2.建立表约束
use Big

--ALTER TABLE Example.REGION DROP PRIMARY KEY;
--ALTER TABLE Example.NATION DROP PRIMARY KEY;
--ALTER TABLE Example.PART DROP PRIMARY KEY;
--ALTER TABLE Example.SUPPLIER DROP PRIMARY KEY;
--ALTER TABLE Example.PARTSUPP DROP PRIMARY KEY;
--ALTER TABLE Example.ORDERS DROP PRIMARY KEY;
--ALTER TABLE Example.LINEITEM DROP PRIMARY KEY;
--ALTER TABLE Example.CUSTOMER DROP PRIMARY KEY;

-- For table REGION
ALTER TABLE dbo.REGION
ADD PRIMARY KEY (R_REGIONKEY);

-- For table NATION
ALTER TABLE dbo.NATION
ADD PRIMARY KEY (N_NATIONKEY);

ALTER TABLE dbo.NATION
ADD constraint NATION_FK1 FOREIGN KEY(N_REGIONKEY) references dbo.REGION(R_REGIONKEY);

 

-- For table PART
ALTER TABLE dbo.PART
ADD PRIMARY KEY (P_PARTKEY);

 

-- For table SUPPLIER
ALTER TABLE dbo.SUPPLIER
ADD PRIMARY KEY (S_SUPPKEY);

ALTER TABLE dbo.SUPPLIER
ADD  constraint SUPPLIER_FK1  FOREIGN KEY(S_NATIONKEY) references dbo.NATION(N_NATIONKEY);

 

-- For table PARTSUPP
ALTER TABLE dbo.PARTSUPP
ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY);

 

-- For table CUSTOMER
ALTER TABLE dbo.CUSTOMER
ADD PRIMARY KEY (C_CUSTKEY);

ALTER TABLE dbo.CUSTOMER
ADD  constraint   CUSTOMER_FK1   FOREIGN KEY (C_NATIONKEY) references dbo.NATION(N_NATIONKEY);

 

-- For table LINEITEM
ALTER TABLE dbo.LINEITEM
ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);

 

-- For table ORDERS
ALTER TABLE dbo.ORDERS
ADD PRIMARY KEY (O_ORDERKEY);

 

-- For table PARTSUPP
ALTER TABLE dbo.PARTSUPP
ADD  constraint PARTSUPP_FK1  FOREIGN KEY  (PS_SUPPKEY) references dbo.SUPPLIER(S_SUPPKEY);

 

ALTER TABLE dbo.PARTSUPP
ADD  constraint  PARTSUPP_FK2   FOREIGN KEY (PS_PARTKEY) references dbo.PART(P_PARTKEY);

 

-- For table ORDERS
ALTER TABLE dbo.ORDERS
ADD   constraint    ORDERS_FK1  FOREIGN KEY (O_CUSTKEY) references dbo.CUSTOMER(C_CUSTKEY);

 

-- For table LINEITEM
ALTER TABLE dbo.LINEITEM
ADD  constraint  LINEITEM_FK1  FOREIGN KEY (L_ORDERKEY)  references dbo.ORDERS(O_ORDERKEY);



ALTER TABLE dbo.LINEITEM
ADD   constraint   LINEITEM_FK2  FOREIGN KEY (L_PARTKEY,L_SUPPKEY) references 
        dbo.PARTSUPP(PS_PARTKEY, PS_SUPPKEY);
