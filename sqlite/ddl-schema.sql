CREATE TABLE part(
  p_partkey INTEGER PRIMARY KEY, p_name, p_mfgr, p_brand, p_type, p_size INTEGER, p_container, p_retailprice REAL, p_comment);

CREATE TABLE supplier(
  s_suppKey INTEGER PRIMARY KEY, s_name, s_address, s_nationkey INTEGER, s_phone, s_acctbal REAL, s_comment);

CREATE TABLE partsupp(
  ps_partKey INTEGER, ps_suppKey INTEGER, ps_availqty  INTEGER, ps_supplycost REAL, ps_comment);

CREATE TABLE customer(
  c_custkey INTEGER PRIMARY KEY, c_name, c_address, c_nationkey INTEGER, c_phone, c_acctbal REAL, c_mktsegment, c_comment);

CREATE TABLE nation(
  n_nationkey INTEGER PRIMARY KEY, n_name, n_regionkey INTEGER, n_comment);

CREATE TABLE region(
  r_regionkey INTEGER PRIMARY KEY, r_name, r_comment);

CREATE TABLE lineitem(
  l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity INTEGER, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment);

CREATE TABLE orders(
  o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_orderstatus, o_totalprice REAL, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment);

CREATE INDEX PartPKI ON part(p_partkey);
CREATE INDEX SupplierPK ON supplier(s_suppkey);
CREATE INDEX PartSuppPKI ON partsupp(ps_partkey, ps_suppkey);
CREATE INDEX CustomerPKI ON customer(c_custkey);
CREATE INDEX NationPKI ON nation(n_nationkey);
CREATE INDEX RegionPKI ON region(r_regionkey);
CREATE INDEX LineItemPKI ON lineitem(l_orderkey, l_linenumber);
CREATE INDEX OrdersPKI ON orders(o_orderkey);
CREATE INDEX SupplierNations ON supplier(s_nationkey);
CREATE INDEX CustomerNations ON customer(c_nationkey);
CREATE INDEX LineItemParts ON lineitem(l_partkey);
CREATE INDEX LineItemSuppliers ON lineitem(l_suppkey);
CREATE INDEX OrderCustomers ON orders(o_custkey);
CREATE INDEX PartSuppSupp ON partsupp(ps_suppkey);
CREATE INDEX OrderDate ON orders(o_orderdate);