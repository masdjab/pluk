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
    super(conn, "customer", Customer)
  end
  def select_query(qp)
    qp.search_fields = "nm_cust, alamat, catatan"
    
    "SELECT id_cust, nm_cust, alamat, catatan, " \
    "f_data#{qp.sql_search_fields} FROM #{table_name}" \
    "#{qp.sql_filter}#{qp.sql_having}#{qp.sql_order_by}#{qp.sql_limit}"
  end
  def count
    self.scalar("SELECT COUNT(*) FROM customer")
  end
end

class PlukTest < Test::Unit::TestCase
  def setup
    @database_name = "pluk_test"
    @connection = Pluk::Connection.new(host: "localhost", username: "root", password: "")
    @connection.create_db(@database_name) unless @connection.db_exist?(@database_name)
    @connection.select_db @database_name
    create_db_struct(@connection) if @connection.get_table_list(@database_name).empty?
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
  #def test_connection
  #  
  #end
  def test_query_params
    qp = Pluk::SelectParams.new
    
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
  def test_model
    cn = @connection
    cm = CustomerModel.new(cn)
    
    cm.truncate
    assert_equal 0, cm.count
    
    create_customers = 
      lambda do
        [
          {nm_cust: "PT Karya", alamat: "Yogyakarta", catatan: "Pak Vektor"}, 
          {nm_cust: "PT Cakra", alamat: "Jakarta", catatan: "Ibu Handayani"}, 
          {nm_cust: "PT Kilat", alamat: "Semarang", catatan: "Ibu Endang"}
        ].each{|c|cm.insert(c)}
      end
    
    # insert
    create_customers.call
    assert_equal 3, cm.connection.last_id
    assert_equal 3, cm.count
    
    
    # load
    assert_equal "PT Karya", cm.load("id_cust = 1").nm_cust
    assert_equal "PT Cakra", cm.load(id_cust: 2).nm_cust
    assert_equal "PT Kilat", cm.load(order_by: "catatan").nm_cust
    
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
    
    cm.update({f_data: 2}, {id_cust: 1})
    assert_equal 1, cm.all(f_data: 2).count
    
    cm.update({f_data: 3}, "", 1)
    assert_equal 1, cm.all(f_data: 3).count
    
    cm.update(f_data: 4)
    assert_equal 3, cm.all(f_data: 4).count
    
    cm.update({f_data: 5}, "", 0)
    assert_equal 3, cm.all(f_data: 5).count
    
    cm.update({f_data: 6})
    assert_equal 3, cm.all(f_data: 6).count
    
    cm.update({f_data: 1})
    assert_equal 3, cm.all(f_data: 1).count
    
    
    # delete
    cm.delete("(id_cust = 1)")
    assert_equal 2, cm.count
    
    cm.delete(id_cust: 2)
    assert_equal 1, cm.count
    
    cm.delete({id_cust: 3})
    assert_equal 0, cm.count
    
    create_customers.call
    assert_equal 3, cm.count
    
    # truncate, empty?
    cm.truncate
    assert_equal 0, cm.count
    
    # exist?
    create_customers.call
    assert_equal 3, cm.count
    assert_equal true, cm.exist?(id_cust: 1)
  end
end
