require 'test/unit'
require '../lib/pluk'


class Customer
  attr_accessor :id_cust, :nm_cust, :alamat, :catatan, :f_data
  
  def initialize
    @id_cust = 0
    @nm_cust = ""
    @alamat = ""
    @catatan = ""
    @f_data = 0
  end
  def self.create(nm_cust, alamat, catatan)
    cust = self.new
    cust.nm_cust = nm_cust
    cust.alamat = alamat
    cust.catatan = catatan
    cust
  end
end

class CustomerModel < Pluk::TableAdapter
  def initialize(conn)
    super(conn, "customer", Customer, true)
  end
  def select_query(qp)
    qp.search_fields = "nm_cust, alamat, catatan"
    
    "SELECT SQL_CALC_FOUND_ROWS id_cust, nm_cust, alamat, catatan, " \
    "f_data#{qp.sql_search_fields} FROM #{table_name}" \
    "#{qp.sql_filter}#{qp.sql_having}#{qp.sql_order_by}#{qp.sql_limit}"
  end
end

class PlukTest < Test::Unit::TestCase
  def db_base_name
    "pluk_test"
  end
  def create_db_struct(conn)
    [
      "
      CREATE TABLE customer(
        id_cust     INT(1) UNSIGNED NOT NULL AUTO_INCREMENT, 
        nm_cust     VARCHAR(30) NOT NULL DEFAULT '', 
        alamat      VARCHAR(80) NOT NULL DEFAULT '', 
        catatan     TEXT, 
        f_data      SMALLINT(1) NOT NULL DEFAULT 0, 
        PRIMARY KEY(id_cust), 
        KEY f_data(f_data)
      )ENGINE=MyISAM
      ", 
      "
      CREATE TABLE produk(
        id_produk   INT(1) UNSIGNED NOT NULL AUTO_INCREMENT, 
        nm_produk   VARCHAR(30) NOT NULL DEFAULT '', 
        PRIMARY KEY(id_produk)
      )ENGINE=MyISAM
      ", 
      "
      CREATE TABLE visit(
        id_visit    INT(1) UNSIGNED NOT NULL AUTO_INCREMENT, 
        id_cust     INT(1) UNSIGNED NOT NULL DEFAULT 0, 
        id_produk   INT(1) UNSIGNED NOT NULL DEFAULT 0, 
        PRIMARY KEY(id_visit), 
        KEY id_cust(id_cust), 
        KEY id_produk(id_produk)
      )ENGINE=MyISAM
      "
    ].each{|c|conn.query(c)}
  end
  def create_connection
    cn = Pluk::Connection.new(hots: "localhost", username: "root", password: "")
    nm = self.db_base_name
    cn.create_db(nm) unless cn.db_exist?(nm)
    cn.select_db nm
    create_db_struct(cn) if cn.get_table_list(nm).empty?
    cn
  end
  def find_free_db_name(conn, base_name)
    db_list = conn.get_database_list
    dbsname = ""
    
    (0..1000).each do |i|
      name = "#{base_name}#{i > 0 ? "_#{i}" : ""}"
      unless db_list.include?(name)
        dbsname = name
        break
      end
    end
    
    if block_given?
      yield dbsname unless dbsname.empty?
    else
      dbsname
    end
  end
  def create_temp_db(conn, db_name)
    find_free_db_name(conn, db_name) do |db|
      conn.query "CREATE DATABASE #{db}"
      conn.select_db db
      create_db_struct conn
      yield db
      conn.query "DROP DATABASE #{db}"
    end
  end
  #def test_connection
  #  
  #end
  def test_query_params
    cn = create_connection
    qp = Pluk::SelectParams.new(cn)
    
    qp.filter = " #{(cr = "(id = 2) AND (name = 'Bowo')")} "
    assert_equal " WHERE #{cr}", qp.sql_filter
    assert_equal " WHERE #{cr}", qp.sql_filter("WHERE")
    assert_equal " WHERE #{cr}", qp.sql_filter("where")
    assert_equal " WHERE #{cr}", qp.sql_filter(" where ")
    assert_equal " AND #{cr}", qp.sql_filter("AND")
    assert_equal " AND #{cr}", qp.sql_filter(" AND ")
    assert_equal cr, qp.sql_filter(" ")
    assert_equal cr, qp.sql_filter("")
    
    # group_by
    qp.group_by = " #{(gb = "part.kd_part, kd_warehouse")} "
    assert_equal " GROUP BY #{gb}", qp.sql_group_by
    assert_equal " GROUP BY #{gb}", qp.sql_group_by("GROUP BY")
    assert_equal " GROUP BY #{gb}", qp.sql_group_by("group by")
    assert_equal " GROUP_BY #{gb}", qp.sql_group_by("group_by")
    assert_equal ", #{gb}", qp.sql_group_by(", ")
    assert_equal ", #{gb}", qp.sql_group_by(",")
    assert_equal gb, qp.sql_group_by(" ")
    assert_equal gb, qp.sql_group_by("")
    
    # having
    qp.having = " #{(hv = "(kd_part = '99002201') AND (kd_kelas = 1)")} "
    assert_equal " HAVING #{hv}", qp.sql_having
    assert_equal " HAVING #{hv}", qp.sql_having("HAVING")
    assert_equal " HAVING #{hv}", qp.sql_having("having")
    assert_equal " HAVING #{hv}", qp.sql_having(" HAVING ")
    assert_equal " AND #{hv}", qp.sql_having("AND")
    assert_equal " AND #{hv}", qp.sql_having("and")
    assert_equal " AND #{hv}", qp.sql_having(" AND ")
    assert_equal hv, qp.sql_having("")
    assert_equal hv, qp.sql_having(" ")
    
    # order_by
    qp.order_by = " #{(ob = "id_order, nm_cust")} "
    assert_equal " ORDER BY #{ob}", qp.sql_order_by
    assert_equal " ORDER BY #{ob}", qp.sql_order_by("ORDER BY")
    assert_equal " ORDER BY #{ob}", qp.sql_order_by(" ORDER BY ")
    assert_equal ", #{ob}", qp.sql_order_by(",")
    assert_equal ", #{ob}", qp.sql_order_by(", ")
    assert_equal ob, qp.sql_order_by(" ")
    assert_equal ob, qp.sql_order_by("")
    
    # offset and limit
    qp.limit = 0
    assert_equal "", qp.sql_limit
    qp.limit = -1
    assert_equal "", qp.sql_limit
    qp.offset = 200
    assert_equal "", qp.sql_limit
    qp.limit = 20
    assert_equal " LIMIT 200, 20", qp.sql_limit
    qp.offset = -10
    assert_equal " LIMIT 0, 20", qp.sql_limit
    
    # search_fields, search_keywords
    sf = "nm_cust, catatan"
    sc = ", CONCAT_WS('|', #{sf}) AS keywords"
    qp.search_fields = " #{sf} "
    qp.search_keywords = ""
    qp.having = " "
    assert_equal "", qp.sql_search_fields
    assert_equal "", qp.sql_having
    qp.having = " #{(hv = "(id_cust = 1)")} "
    assert_equal "", qp.sql_search_fields
    assert_equal " HAVING #{hv}", qp.sql_having
    qp.search_keywords = " #{(kw = "bowo, yudi")} "
    kc = kw.lines(",").map{|x|x.strip}.map{|x|"(keywords LIKE '%#{x}%')"}.join(" AND ")
    assert_equal sc, qp.sql_search_fields
    assert_equal " HAVING #{hv} AND #{kc}", qp.sql_having
    qp.search_fields = "  "
    assert_equal "", qp.sql_search_fields
    assert_equal " HAVING #{hv}", qp.sql_having
  end
  def test_sqlval
    cn = create_connection
    qa = Pluk::QueryAdapter.new(cn, "customer")
    
    assert_equal "NULL", qa.sqlval(nil)
    assert_equal "\"1\"", qa.sqlval(1)
    assert_equal "\"2.5\"", qa.sqlval(2.5)
    assert_equal "\"Heryudi Praja\"", qa.sqlval("Heryudi Praja")
    assert_equal "\"2019-04-23 10:20:30\"", qa.sqlval(Time.new(2019, 4, 23, 10, 20, 30))
    assert_equal "CURRENT_TIMESTAMP", qa.sqlval(Pluk::SQLFunction("CURRENT_TIMESTAMP"))
    assert_equal "MD5('<*admin*>')", qa.sqlval(Pluk::SQLFunction("MD5('<*admin*>')"))
    assert_equal "IF(f_aprv > 0, 1, 0)", qa.sqlval(Pluk::SQLFunction("IF(f_aprv > 0, 1, 0)"))
  end
  def test_select_params
    cn = create_connection
    qa = Pluk::QueryAdapter.new(cn, "customer")
    kk = [:id_cust, :nm_cust, :f_data]
    
    # map_params, field_maps
    assert_equal [], qa.map_params(nil, kk)
    pr = {id_cust: 21}
    assert_equal ["(`customer`.`id_cust` = \"21\")"], qa.map_params(pr, kk)
    pr = {id_cust: 21, nm_cust: "Ma'aruf"}
    assert_equal ["(`customer`.`id_cust` = \"21\")", "(`customer`.`nm_cust` = \"Ma'aruf\")"], qa.map_params(pr, kk)
    pr = {id_cust: 21, nm_cust: "Ma\"aruf"}
    assert_equal ["(`customer`.`id_cust` = \"21\")", "(`customer`.`nm_cust` = \"Ma\"aruf\")"], qa.map_params(pr, kk)
    pr = {id_cust: 21, alamat: "Yogya"}
    assert_equal ["(`customer`.`id_cust` = \"21\")"], qa.map_params(pr, kk)
    pr = {id_cust: 21}
    qa.field_maps = {id_cust: "ms_customer.id_customer"}
    assert_equal ["(ms_customer.id_customer = \"21\")"], qa.map_params(pr, kk)
    
    # map_values
    
    # select_params
    qa.field_maps = {}
    sp = qa.select_params()
    assert_equal "", sp.sql_filter
    assert_equal "", sp.sql_group_by
    assert_equal "", sp.sql_having
    assert_equal "", sp.sql_order_by
    assert_equal "", sp.sql_limit
    
    sp = qa.select_params("id_cust = 1")
    assert_equal " WHERE (id_cust = 1)", sp.sql_filter
    
    sp = qa.select_params("id_cust = 1 AND flag = 1")
    assert_equal " WHERE (id_cust = 1 AND flag = 1)", sp.sql_filter
    
    sp = qa.select_params("(id_cust = 1) AND (flag = 1)")
    assert_equal " WHERE (id_cust = 1) AND (flag = 1)", sp.sql_filter
    
    sp = qa.select_params("id_cust = 1", order_by: "nm_cust, id_cust")
    assert_equal " WHERE (id_cust = 1)", sp.sql_filter
    assert_equal " ORDER BY nm_cust, id_cust", sp.sql_order_by
    
    sp = qa.select_params(id_cust: 1)
    assert_equal " WHERE (`customer`.`id_cust` = \"1\")", sp.sql_filter
    
    sp = qa.select_params(id_cust: 1, order_by: "nm_cust DESC")
    assert_equal " WHERE (`customer`.`id_cust` = \"1\")", sp.sql_filter
    assert_equal " ORDER BY nm_cust DESC", sp.sql_order_by
    
    qa.field_maps = {id_cust: "ms_customer.id_customer"}
    sp = qa.select_params(id_cust: 1, order_by: "nm_cust DESC")
    assert_equal " WHERE (ms_customer.id_customer = \"1\")", sp.sql_filter
    
    qa.field_maps = {}
    sp = qa.select_params(id_cust: 1, f_data: 2)
    assert_equal " WHERE (`customer`.`id_cust` = \"1\") AND (`customer`.`f_data` = \"2\")", sp.sql_filter
    
    sp = qa.select_params(id_cust: 1, f_data: 2, having: "(f_deleted = 0)")
    assert_equal " WHERE (`customer`.`id_cust` = \"1\") AND (`customer`.`f_data` = \"2\")", sp.sql_filter
    assert_equal " HAVING (f_deleted = 0)", sp.sql_having
    
    sp = qa.select_params(id_cust: 1, having: {f_data: 0})
    assert_equal " WHERE (`customer`.`id_cust` = \"1\")", sp.sql_filter
    assert_equal " HAVING (`customer`.`f_data` = \"0\")", sp.sql_having
    
    sp = qa.select_params(offset: 20)
    assert_equal "", sp.sql_limit
    
    sp = qa.select_params(limit: 10)
    assert_equal " LIMIT 0, 10", sp.sql_limit
    
    sp = qa.select_params(limit: 10, offset: 20)
    assert_equal " LIMIT 20, 10", sp.sql_limit
    
    sp = qa.select_params(limit: -10, offset: -20)
    assert_equal "", sp.sql_limit
    
    sp = qa.select_params(limit: 10, offset: -20)
    assert_equal " LIMIT 0, 10", sp.sql_limit
    
    sp = qa.select_params(id_cust: 1, f_data: 2, having: "jml_beli > 0", order_by: "nm_cust ASC, id_cust", offset: 200, limit: 20)
    assert_equal " WHERE (`customer`.`id_cust` = \"1\") AND (`customer`.`f_data` = \"2\")", sp.sql_filter
    assert_equal " HAVING jml_beli > 0", sp.sql_having
    assert_equal " ORDER BY nm_cust ASC, id_cust", sp.sql_order_by
    assert_equal " LIMIT 200, 20", sp.sql_limit
  end
  def test_model
    cn = create_connection
    cm = CustomerModel.new(cn)
    
    cm.truncate
    assert_equal 0, cm.count
    
    # insert
    [
      {nm_cust: "PT Karya", alamat: "Yogyakarta", catatan: "Pak Vektor"}, 
      {nm_cust: "PT Cakra", alamat: "Jakarta", catatan: "Ibu Handayani"}, 
      {nm_cust: "PT Kilat", alamat: "Semarang", catatan: "Ibu Endang"}
    ].each{|c|cm.insert(c)}
    assert_equal 3, cm.connection.last_id
    assert_equal 3, cm.count
    
    
    cm.truncate
    create_customers = 
      lambda do
        [
          Customer.create("PT Karya", "Yogyakarta", "Pak Vektor"), 
          Customer.create("PT Cakra", "Jakarta", "Ibu Handayani"), 
          Customer.create("PT Kilat", "Semarang", "Ibu Endang")
        ].each{|c|cm.insert(c)}
      end
    create_customers.call
    assert_equal 3, cm.connection.last_id
    assert_equal 3, cm.count
    
    
    # all
    assert_equal true, (cm.all.map{|x|x.nm_cust} - ["PT Karya", "PT Cakra", "PT Kilat"]).empty?
    assert_equal 3, cm.found_rows
    
    
    # first
    assert_equal "Customer", cm.first.class.to_s
    assert_equal "PT Karya", cm.first("id_cust = 1").nm_cust
    assert_equal "PT Cakra", cm.first(id_cust: 2).nm_cust
    assert_equal "PT Kilat", cm.first(order_by: "catatan").nm_cust
    
    
    # load
    cu = cm.load({id_cust: 1})
    assert_equal "1|PT Karya", "#{cu.id_cust}|#{cu.nm_cust}"
    
    cu = cm.load(id_cust: 1)
    assert_equal "1|PT Karya", "#{cu.id_cust}|#{cu.nm_cust}"
    
    cu = cm.load(filter: {id_cust: 1})
    assert_equal "1|PT Karya", "#{cu.id_cust}|#{cu.nm_cust}"
    
    cu = cm.load("(id_cust = 1)")
    assert_equal "1|PT Karya", "#{cu.id_cust}|#{cu.nm_cust}"
    
    
    # update
    cm.update({nm_cust: "PT Kasai"}, "(id_cust = 1)")
    assert_equal "PT Kasai", cm.load("(id_cust = 1)").nm_cust
    
    cu.nm_cust = "PT Karye"
    cm.update(cu, "(id_cust = 1)")
    assert_equal "PT Karye", cm.load(id_cust: 1).nm_cust
    
    cu.nm_cust = "PT Karye"
    cm.update(cu, id_cust: 1)
    assert_equal "PT Karye", cm.load(id_cust: 1).nm_cust
    
    cu.nm_cust = "PT Karyu"
    cm.update(cu, {id_cust: 1})
    assert_equal "PT Karyu", cm.load(id_cust: 1).nm_cust
    
    cm.update({f_data: 2}, {id_cust: 1})
    assert_equal 1, cm.all(f_data: 2).count
    
    cm.update({f_data: 3}, "", 1)
    assert_equal 1, cm.all(f_data: 3).count
    
    cm.update(f_data: 4)
    assert_equal 3, cm.all(f_data: 4).count
    
    cm.update({f_data: 5}, "", nil)
    assert_equal 1, cm.all(f_data: 5).count
    
    cm.update({f_data: 6}, "", [])
    assert_equal 1, cm.all(f_data: 6).count
    
    cm.update({f_data: 1}, "", 0)
    assert_equal 3, cm.all(f_data: 1).count
    
    
    # load_to
    cm.load_to((cx = Customer.new), "(id_cust = 2)")
    assert_equal "2|PT Cakra", "#{cx.id_cust}|#{cx.nm_cust}"
    
    cm.load_to((cx = Customer.new), id_cust: 2)
    assert_equal "2|PT Cakra", "#{cx.id_cust}|#{cx.nm_cust}"
    
    cm.load_to((cx = Customer.new), {id_cust: 2})
    assert_equal "2|PT Cakra", "#{cx.id_cust}|#{cx.nm_cust}"
    
    cm.load_to((cx = Customer.new), filter: {id_cust: 2})
    assert_equal "2|PT Cakra", "#{cx.id_cust}|#{cx.nm_cust}"
    
    
    # delete
    cm.delete("(id_cust = 1)")
    assert_equal 2, cm.count
    
    cm.delete(id_cust: 2)
    assert_equal 1, cm.count
    
    cm.delete({id_cust: 3})
    assert_equal 0, cm.count
    
    create_customers.call
    assert_equal 3, cm.count
    
    cm.delete nil, 1
    assert_equal 2, cm.count
    
    cm.delete
    assert_equal 0, cm.count
    
    # truncate, empty?
    cm.truncate
    assert_equal true, cm.empty?
    
    # exist?
    create_customers.call
    assert_equal 3, cm.count
    assert_equal true, cm.exist?(id_cust: 1)
  end
end
