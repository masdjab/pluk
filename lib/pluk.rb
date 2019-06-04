# Pluk, written by Heryudi Praja (mr_orche@yahoo.com)
# future improvements plan:
# - query can output an array instead of object

require 'mysql2'

module Pluk
  Version   = "1.0.0.20"
  BuildDate = "190604a"
  
  class SQLFunction
    def initialize(expr)
      @expression = expr
    end
    def sql_value_syntax
      "#{@expression}"
    end
  end
  
  
  class SQLValue
    def initialize(value)
      @value = value
    end
    def sql_value_syntax
      if @value.respond_to?(:sql_value_syntax)
        @value.sql_value_syntax
      elsif @value.nil?
        "NULL"
      elsif @value.is_a?(Time)
        "\"#{@value.strftime("%Y-%m-%d %H:%M:%S")}\""
      elsif @value.is_a?(String)
        "'#{Mysql2::Client.escape(@value)}'"
      else
        "\"#{@value}\""
      end
    end
  end
  
  
  def self.SQLFunction(expr)
    SQLFunction.new(expr)
  end
  
  
  class SQLField
    attr_reader :table, :name, :type, :null, :key, :default, :extra
    
    def initialize(table, name, type, null, key, default, extra)
      @table    = table
      @name     = name
      @type     = type
      @null     = null
      @key      = key
      @default  = default
      @extra    = extra
    end
    def sql_value_syntax
      "`#{@table}`.`#{@name}`"
    end
  end
  
  
  class ConnectionPool
    attr_accessor :max_connections, :max_retries, :retry_interval
    
    def initialize(options)
      if options.is_a?(Hash)
        @options = options
        @max_connections = 10
        @max_timeout = 10
        @retry_interval = 0.5
        @free_conns = []
        @used_count = 0
        @last_id = nil
      else
        raise "Parameter 'options' for #{self.class}.new must be a Hash."
      end
    end
    def host
      @options[:host]
    end
    def user
      @options[:user]
    end
    def password
      @options[:password]
    end
    def database
      @options[:database]
    end
    def used_count
      @used_count
    end
    def free_count
      @free_conns.count
    end
    def total_conns
      used_count + free_count
    end
    def request_conn
      drop = nil
      conn = nil
      atts = 0
      
      if conn.nil?
        if !@free_conns.empty?
          tc = @free_conns.shift
          if tc.ping
            conn = tc if tc.ping
          else
            @free_conns.clear
          end
        end
      end
      
      if conn.nil?
        t1 = Time.new
        et = t1 + @max_timeout
        
        loop do
          begin
            atts += 1
            tc = Mysql2::Client.new(@options) if @free_conns.empty?
            if tc.ping
              conn = tc
              break
            end
          rescue
            if drop .nil?
              drop = true
            end
            sleep @retry_interval
          end
          
          break if Time.new > et
        end
        
        t2 = Time.new
        
        if conn.nil?
          raise "Get mysql connection pool failed after #{t2 - t1} secs."
        end
      end
      
      conn
    end
    def with_engine
      if used_count >= @max_connections
        raise "Maximum connection pools (#{@max_connections}) reached."
      else
        if conn = request_conn
          @used_count += 1
          yield conn
          @free_conns << conn
          @used_count -= 1
        end
      end
    end
    def last_id
      @last_id
    end
    def escape(t)
      rt = nil
      with_engine{|e|rt = e.escape(t)}
      rt
    end
    def query(c)
      rt = nil
      
      with_engine do |e|
        rt = e.query(c)
        
        begin
          @last_id = e.last_id if !e.last_id.nil? && e.last_id > 0
          @affected_rows = e.affected_rows if e.affected_rows
        rescue
        end
      end
      
      rt
    end
    def scalar(c)
      row = query(c).first
      itm = row ? row[row.keys[0]] : nil
    end
    def create_db(name)
      self.query("CREATE DATABASE `#{name}`")
    end
    def select_db(name)
      with_engine{|e|e.select_db(name)}
    end
    def affected_rows
      @affected_rows
    end
  end
  
  
  class Connection
    def initialize(options = {})
      @mysql = ConnectionPool.new(options)
    end
    def escape(text)
      @mysql.escape(text)
    end
    def query(cmd)
      begin
        @mysql.query(cmd)
      rescue Exception => ex
        puts "Error executing following SQL:\n#{cmd}\n#{ex.message}"
        raise ex.message
      end
    end
    def scalar(cmd)
      begin
        @mysql.scalar(cmd)
      rescue Exception => ex
        puts "Error executing following SQL:\n#{cmd}\n#{ex.message}"
        raise ex.message
      end
    end
    def create_db(name)
      self.query("CREATE DATABASE `#{name}`")
    end
    def select_db(name)
      @mysql.select_db name
    end
    def db_exist?(name)
      !get_database_list(name).empty?
    end
    def affected_rows
      @mysql.affected_rows
    end
    def last_id
      @mysql.last_id
    end
    def info
      @mysql.info
    end
    def get_field_list(table, database = nil)
      tt = database ? "#{database}." : ""
      cc = self.query("SHOW COLUMNS FROM #{tt}#{table}").map{|x|x}
      cc.map do |x|
        SQLField.new(
          table, 
          x["Field"].to_sym, 
          x["Type"].to_sym, 
          x["Null"] == "YES", 
          !x["Key"].empty? ? x["Key"] : nil, 
          x["Default"], 
          !x["Extra"].empty? ? x["Extra"] : nil
        )
      end
    end
    def get_table_list(database = nil)
      tt = database ? " FROM `#{database.to_s}`" : ""
      self.query("SHOW TABLES#{tt}").map{|x|x[x.keys[0]]}
    end
    def get_database_list(name = "", match_pattern = false)
      cc = !name.empty? ? " LIKE " + (match_pattern ? "'%#{name}%'" : "'#{name}'") : ""
      self.query("SHOW DATABASES#{cc}").map{|x|x["Database"]}
    end
  end
  
  
  class ColumnOrder
    attr_accessor :column, :dir
    
    def initialize(column, dir)
      @column = column
      @dir = dir
    end
    def sql_syntax
      "`#{@column}`#{@dir != :asc ? " DESC" : ""}"
    end
    def self.asc(column)
      self.new(column, :asc)
    end
    def self.desc(column)
      self.new(column, :desc)
    end
  end
  
  
  class QueryParams
    private
    def escape(text)
      Mysql2::Client.escape(text)
    end
    def extract_words(text, separator)
      kk = text.empty? ? [] : text.strip.lines(separator)
      kk.map{|x|x.chomp(separator).strip}.select{|x|!x.empty?}
    end
  end
  
  
  class SelectParams < QueryParams
    attr_accessor \
      :search_fields, :search_keywords, :filter, 
      :group_by, :having, :order_by, :offset, :limit
    
    private
    def initialize(options = {})
      oo                    = options || {}
      self.search_fields    = oo.delete(:search_fields){|k|""}
      self.search_keywords  = oo.delete(:keywords){|k|""}
      self.filter           = oo.delete(:filter){|k|""}
      self.group_by         = oo.delete(:group_by){|k|""}
      self.having           = oo.delete(:having){|k|""}
      self.order_by         = oo.delete(:order_by){|k|""}
      self.offset           = oo.delete(:offset){|k|0}
      self.limit            = oo.delete(:limit){|k|0}
    end
    def make_clause(clause)
      if (cx = clause.strip).empty?
        ""
      elsif cx == ","
        ", "
      else
        " #{cx.upcase} "
      end
    end
    
    public
    def search_fields=(data)
      @search_fields = data.is_a?(String) ? data.strip : ""
    end
    def search_keywords=(data)
      @search_keywords = data.is_a?(String) ? data.strip : ""
    end
    def filter=(data)
      @filter = data.is_a?(String) ? data.strip : ""
    end
    def group_by=(data)
      @group_by = data.is_a?(String) ? data.strip : ""
    end
    def having=(data)
      @having = data.is_a?(String) ? data.strip : ""
    end
    def order_by=(data)
      @order_by = data.is_a?(String) ? data.strip : ""
    end
    def offset=(data)
      @offset = data.is_a?(Integer) && (data >= 0) ? data : 0
    end
    def limit=(data)
      @limit = data.is_a?(Integer) && (data > 0) ? data : 0
    end
    def sql_search_fields
      sf = @search_fields.strip
      sk = @search_keywords.strip
      
      if !sf.empty? && !sk.empty?
        ", CONCAT_WS('|', #{sf}) AS keywords"
      else
        ""
      end
    end
    def sql_filter(clause = "WHERE")
      !@filter.empty? ? "#{make_clause(clause)}#{@filter}" : ""
    end
    def sql_group_by(clause = "GROUP BY")
      !@group_by.empty? ? "#{make_clause(clause)}#{@group_by}" : ""
    end
    def sql_having(clause = "HAVING")
      if !@search_fields.empty? && !@search_keywords.empty?
        kk = 
          extract_words(@search_keywords, " ")
          .map{|x|"(keywords LIKE '%#{escape(x)}%')"}
        hh = ([@having] + kk).select{|x|!x.empty?}.join(" AND ")
      else
        hh = @having
      end
      
      (!hh.empty? ? "#{make_clause(clause)}#{hh}" : "")
    end
    def sql_order_by(clause = "ORDER BY")
      !@order_by.empty? ? "#{make_clause(clause)}#{@order_by}" : ""
    end
    def sql_limit
      (@offset >= 0) && (@limit > 0) ? " LIMIT #{@offset}, #{@limit}" : ""
    end
    def self.create(connection, args = nil)
      if args.nil?
        self.new(connection, {})
      elsif args.is_a?(Hash)
        self.new(connection, args)
      else
        raise "#{self}.create expect argument-2 is a Hash or nil, #{args.class} given."
      end
    end
  end
  
  
  class DefaultOutputter
    def output(items)
      items
    end
  end
  
  
  class ObjectOutputter
    attr_reader :output_type
    def initialize(output_type)
      @output_type = output_type
    end
    def output(items)
      items.map do |r|
        tmp = @output_type.new
        r.keys.each do |k|
          mm = :"#{k}="
          tmp.__send__(mm, r[k]) if tmp.respond_to?(mm)
        end
        tmp
      end
    end
  end
  
  
  class QueryAdapter
    attr_accessor :field_maps
    attr_reader   :connection, :table_name, :column_hash
    
    private
    def initialize(connection, table_name)
      @connection = connection
      @table_name = table_name
      @column_hash = @connection.get_field_list(@table_name).inject({}){|a,b|a[b.name] = b; a}
      @outputter = DefaultOutputter.new
      @field_maps = {}
    end
    def sym_keys(h)
      h.keys.inject({}) do |a, b|
        v = h[b]
        if v.is_a?(Hash)
          v = sym_keys(v)
        elsif v.is_a?(Array)
          v = 
            v.map do |x|
              if x.is_a?(Hash)
                sym_keys(x)
              else
                x
              end
            end
        end
        
        a[b.to_sym] = v
        a
      end
    end
    def combine_hash(*args)
      args.inject{|a,b|a = (a ? a : {}).merge(b ? b : {});a}
    end
    
    public
    def select_query(args)
    end
    def sqlval(value)
      Pluk::SQLValue.new(value).sql_value_syntax
    end
    def map_fields(maps)
      @field_maps.merge!(maps)
    end
    def map_params(params, keys)
      # map non-string params filtered by keys into string array
      
      if params.is_a?(Hash)
        params.select{|k,v|keys.include?(k) && !v.nil?}
        .map{|k,v|"(#{@field_maps.fetch(k, "`#{@table_name}`.`#{k}`")} = #{sqlval(v)})"}
      else
        []
      end
    end
    def map_values(filters, params)
      # convert non-string filters to string array
      filters + (params.is_a?(Hash) ? map_params(params, @column_hash.keys + @field_maps.keys) : [])
    end
    def select_params(filter = "", options = {})
      # usage examples:
      # select_params()
      # select_params(limit: 20)
      # select_params("id_customer = 1", limit: 20)
      # select_params(filter: "id_customer = 1", limit :20)
      # select_params(filter: {id_customer: 1, f_data: 1}, limit: 20)
      # select_params(id_customer: 1, limit: 20)
      # select_params(id_customer: 1, having: "(f_data = 1)", offset: 20, limit: 10)
      # select_params(id_customer: 1, having: {f_data: 1}, offset: 20, limit: 10)
      
      # convert filter to string
      options, filter = filter, "" if filter.is_a?(Hash)
      
      fx = map_values([], options)
      
      if filter.is_a?(String)
        if !(filter = filter.strip).empty?
          if (filter[0] != "(") || (filter[-1] != ")")
            filter = "(#{filter})"
          end
          fx << filter.strip
        end
      else
        fx = map_values(fx, filter)
      end
      
      if !(ff = options.delete(:filter){|k|""}).is_a?(String)
        fx = map_values(fx, ff)
      end
      
      options[:filter] = fx.select{|x|!x.empty?}.join(" AND ")
      
      
      # convert having to string
      if (having = options[:having])
        if !having.is_a?(String)
          options[:having] = map_values([], having).select{|x|!x.empty?}.join(" AND ")
        end
      end
      
      
      # create SelectParams
      SelectParams.new(options)
    end
    def select_all(filter = "", options = {})
      @connection.query(select_query(select_params(filter, options)))
    end
    def count
      @connection.scalar("SELECT COUNT(*) AS items_count FROM `#{@table_name}`")
    end
    def empty?
      self.count == 0
    end
    def all(filter = "", options = {})
      @outputter.output select_all(filter, options)
    end
    def first(filter = "", options = {})
      all(filter, combine_hash(options, limit: 1)).first
    end
    def load(filter, options = {})
      first(filter, options)
    end
    def exist?(filter = "", options = {})
      !first(filter, options).nil?
    end
    def insert(data)
      # insert a Hash value into table
      
      f, v = [], []
      
      data.each do |x, y|
        f << "`#{x}`"
        v << SQLValue.new(y).sql_value_syntax
      end
      
      @connection.query(
        "INSERT INTO `#{@table_name}`(#{f.join(", ")}) VALUES(#{v.join(", ")})"
      )
    end
    def update(data, filter = "", limit = 0)
      lm = !limit.is_a?(Integer) || (limit < 0) ? 1 : limit
      
      if filter.nil?
        ff = ""
      elsif filter.is_a?(String)
        ff = filter.strip
      else
        ff = map_values([], filter).select{|x|!x.empty?}.join(" AND ")
      end
      
      sc = 
        data
        .map{|k,v| "`#{k}` = #{SQLValue.new(v).sql_value_syntax}"}
        .join(", ")
      
      @connection.query(
        "UPDATE `#{@table_name}` SET #{sc}" \
        "#{!ff.empty? ? " WHERE #{ff}" : ""}" \
        "#{lm > 0 ? " LIMIT #{lm}" : ""}"
      )
    end
    def delete(filter = "", limit = 0)
      lm = !limit.is_a?(Integer) || (limit < 0) ? 1 : limit
      
      if filter.nil?
        ff = ""
      elsif filter.is_a?(String)
        ff = filter.strip
      else
        ff = map_values([], filter).select{|x|!x.empty?}.join(" AND ")
      end
      
      @connection.query(
        "DELETE FROM `#{@table_name}`" \
        "#{!ff.empty? ? " WHERE #{ff}" : ""}" \
        "#{lm > 0 ? " LIMIT #{lm}" : ""}"
      )
    end
    def truncate
      @connection.query "TRUNCATE `#{@table_name}`"
    end
  end
  
  
  class TableAdapter < QueryAdapter
    attr_reader :connection, :type, :table_name, :column_hash, :found_rows
    
    def initialize(connection, table_name, output_type = nil, calc_found_rows = false)
      super(connection, table_name)
      @output_type = output_type
      @outputter = ObjectOutputter.new(@output_type) if !@output_type.nil?
      @calc_found_rows = calc_found_rows
      @found_rows = -1
    end
    def all(filter = "", options = {})
      @found_rows = -1
      rows = super(filter, options)
      @found_rows = @connection.scalar("SELECT FOUND_ROWS()") if @calc_found_rows
      rows
    end
    def load_to(target, filter = "", options = {})
      if @output_type.nil?
        raise "#{self.class}.load_to cannot be used because output_type is not defined."
      elsif !target.is_a?(@output_type)
        raise "#{self.class}.load_to expect first parameter is a #{@output_type}, #{target.class} given."
      else
        oo = load(filter, options)
        @column_hash.keys.each do |k|
          rr = :"#{k}"
          ww = :"#{k}="
          target.__send__(ww, oo.__send__(rr))
        end
      end
    end
    def insert(data)
      if !@output_type.nil? && data.is_a?(@output_type)
        temp = 
          @column_hash.keys.inject({}) do |a, b|
            a[b] = data.__send__(b.to_sym); a
          end
        super(temp)
      else
        super(data)
      end
    end
    def update(data, filter = "", limit = 0)
      if !@output_type.nil? && data.is_a?(@output_type)
        temp = 
          @column_hash.keys.inject({}) do |a, b|
            a[b] = data.__send__(b.to_sym); a
          end
        super(temp, filter, 1)
      else
        super(data, filter, limit)
      end
    end
  end
end
