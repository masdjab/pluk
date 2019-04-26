<html>
<body>
  <h1>Pluk v1.0.0.13</h1>
  
  A simple ruby gem to simplify MySQL queries.<br/><br/>
  
  Usage examples:<br/>
  <pre>
require 'pluk'

class User
  attr_accessor :user_id, :first_name, :last_name, :department_id
end

class Users < Pluk::TableAdapter
  def initialize(conn)
    super(conn, "users", User)
  end
  def select_query(qp)
    qp.search_fields = "first_name, last_name"
    
    "SELECT user_id, first_name, last_name, department_id#{qp.sql_search_fields} " \
    "FROM users #{qp.sql_filter}#{qp.sql_having}#{qp.sql_order_by}#{qp.sql_limit}"
  end
end


cn = Pluk::Connection.new(host: "localhost", username: "root", password: "", database: "pluk-test")
uu = Users.new(cn)

# retrieve all users (return array):
uu.all

# retrieve users by department_id (return array):
uu.all(department_id: 4)

# retrieve users by department_id (return array):
uu.all("department_id = 4")

# retrieve users using keywords (return array):
uu.all(keywords: "heryudi praja")

# retrieve a user by id (return single object):
uu.first(user_id: 1)

# retrieve a user by id (return single object):
uu.first("user_id = 1")

# sort results
uu.all(order_by: "first_name, last_name")

# more select
uu.all(department_id: 1, order_by: "last_name", offset: 10, limit: 20, keywords: "yudi")

# load to object
u1 = uu.load(user_id: 1)

# load_to
u1 = User.new
uu.load_to u1, "(user_id = 2)")

# load_to
u1 = User.new
uu.load_to u1, user_id: 2

# clear table
uu.truncate

# insert from hash
[
  {first_name: "Heryudi", last_name: "Praja", department_id: 1}, 
  {first_name: "Ki", last_name: "Sanak", department_id: 1}, 
  {first_name: "Si", last_name: "Fulan", department_id: 2}
].each{|c|uu.insert(c)}

# insert from object
u1 = User.new
u1.first_name = "Heryudi"
u1.last_name = "Praja"
u1.department_id = 2
uu.insert u1

# update
uu.update({first_name: "Someone", last_name: "Else"}, "(id_user = 1)")

# update from object
u1 = uu.load(user_id: 1)
u1.first_name = "Someone"
u1.last_name = "Else"
uu.update u1, user_id: 1

# update with limit of 10 rows
uu.update({note: "invalid account"}, {f_data: 0}, 10)

# delete
uu.delete "(user_id = 1)"

# delete
uu.delete user_id: 1

# delete with limit of 10 rows
uu.delete {f_data: 0}, 10

# empty?
uu.empty?

# exist?
uu.exist? user_id: 1
  </pre>
</body>
</html>
