# Pluk, written by Heryudi Praja (mr_orche@yahoo.com)
# future improvements plan:
# - query can output an array instead of object

require 'mysql2'

module Pluk
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
  
  
  class Connection
    attr_reader :busy, :affected_rows, :last_id
    
    def initialize(options)
      @options = options
      @pools = {}
      @busy = false
      @affected_rows = nil
      @last_id = nil
    end
    def self.get_object(obj_id)
      ObjectSpace._id2ref(obj_id) rescue nil
    end
    def with_conn(&block)
      if block_given?
        free = @pools.keys.select{|k|@pools[k].nil?}
        if !free.empty?
          mysql = free.first
        else
          mysql = Mysql2::Client.new(@options)
        end
        
        @pools[mysql] = 1
        @busy = true
        res = yield(mysql)
        @busy = false
        @pools[mysql] = nil
        res
      end
    end
    def batch_mode(&block)
      # deprecated, use with_conn instead
      with_conn(&block)
    end
    def pools_info
      {
         free: @pools.keys.select{|k|@pools[k].nil?}.count, 
         used: @pools.keys.select{|k|!@pools[k].nil?}.count, 
        total: @pools.count
      }
    end
    def escape(text)
      Mysql2::Client.escape(text)
    end
    def ping
      with_conn{|c|c.ping}
    end
    def query(cmd)
      with_conn do |c|
        begin
          res = c.query(cmd)
          @affected_rows = c.affected_rows
          @last_id = c.last_id
          res
        rescue Exception => ex
          puts "Error executing following SQL:\n#{cmd}\n#{ex.message}"
          raise ex.message
        end
      end
    end
    def scalar(cmd)
      rs = query(cmd)
      !rs.nil? && !rs.first.nil? ? rs.first[rs.first.keys[0]] : nil
    end
    def create_db(name)
      self.query("CREATE DATABASE `#{name}`")
    end
    def select_db(name)
      with_conn{|c|c.select_db name}
    end
    def db_exist?(name)
      !get_database_list(name).empty?
    end
    def info
      with_conn{|c|c.info}
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
  
  
  class SelectParams
    attr_accessor \
      :search_fields, :search_keywords, :filter, 
      :group_by, :having, :order_by, :offset, :limit
    
    private
    def initialize(options = {})
      self.search_fields    = options.fetch(:search_fields, "")
      self.search_keywords  = options.fetch(:keywords, "")
      self.filter           = options.fetch(:filter, "")
      self.group_by         = options.fetch(:group_by, "")
      self.having           = options.fetch(:having, "")
      self.order_by         = options.fetch(:order_by, "")
      self.offset           = options.fetch(:offset, 0)
      self.limit            = options.fetch(:limit, 0)
    end
    def escape(t)
      Mysql2::Client.escape(t)
    end
    def extract_words(text, separator)
      kk = text.empty? ? [] : text.strip.lines(separator)
      kk.map{|x|x.chomp(separator).strip}.select{|x|!x.empty?}
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
  end
  
  
  class HashResolver
    private
    def initialize(table, params)
      @table = table
      @params = params
    end
    
    public
    def resolve(separator, pf = "", sf = "")
      if !@params.is_a?(Hash)
        raise "#{self.class}.resolve => @params must be a Hash, #{@params.class} given."
      else
        r1 = 
          @params.map do |k,v|
            k1 = "#{k}"
            k2 = k1.include?("`") || k1.include?(".") ? k1 : "`#{k1}`"
            "#{pf}#{k2} #{v.nil? ? "IS" : "="} #{SQLValue.new(v).sql_value_syntax}#{sf}"
          end
        r1.join(separator)
      end
    end
    def fields(separator = ", ")
      r1 = 
        @params.keys.map do |x|
          x1 = "#{x}"
          "#{x1.include?("`") || x1.include?(".") ? x1 : "`#{x1}`"}"
        end
      r1.join(separator)
    end
    def values(separator = ", ")
      @params.values.map{|x|SQLValue.new(x).sql_value_syntax}.join(separator)
    end
    def criteria(separator = " AND ", pf = "(", sf = ")")
      resolve separator, pf, sf
    end
    def fields_values(separator = ", ")
      resolve separator
    end
  end
  
  
  class TableAdapter
    attr_accessor :on_query
    attr_reader   :connection, :table_name, :last_id, :field_maps, :output_type
    
    private
    def initialize(conn, table_name, output_type = nil, options = {})
      @connection   = conn
      @table_name   = table_name
      @output_type  = output_type
      @extra_fields = []
      @symbolize    = options.fetch(:symbolize, true)
      @column_hash  = nil
      @last_id      = nil
      @field_maps   = {}
      @on_query     = nil
    end
    
    public
    def busy
      @connection.busy
    end
    def batch_mode(&block)
      @connection.batch_mode(&block)
    end
    def detect_columns
      @column_hash = 
        self.connection.get_field_list(@table_name)
        .inject({}){|a,b|a[b.name] = b; a}
    end
    def column_hash
      detect_columns if @column_hash.nil?
      @column_hash
    end
    def column_names
      column_hash.keys
    end
    def map_fields(params)
      if !params.is_a?(Hash)
        raise "#{self.class}.map_fields => params must be a Hash."
      else
        @field_maps.merge!(params.sym_keys)
      end
    end
    def allow_fields(*args)
      @extra_fields += args.map{|x|x.to_sym}
    end
    def output(result)
      if !result.nil?
        if !@output_type.nil?
          result.map do |r|
            obj = @output_type.new
            
            r.each do |k,v|
              if obj.respond_to?(sm = :"#{k}=")
                obj.__send__ sm, v
              end
            end
            
            obj
          end
        elsif @symbolize
          result.map{|x|x.sym_keys}
        else
          result
        end
      else
        nil
      end
    end
    def query(sql)
      @on_query.call(@connection, sql) if !@on_query.nil?
      @connection.query(sql)
    end
    def exec(sql)
      @connection.query(sql)
    end
    def select_query(params)
    end
    def escape(t)
      Mysql2::Client.escape(t)
    end
    def select_params(params)
      if params.is_a?(Hash)
        rw = [:search_fields, :search_keywords, :keywords, :group_by, :having, :order_by, :offset, :limit]
        rp = params.keys.inject({}){|a,b|a[b] = params[b];a}
        px = rw.inject({}){|a,b|a[b] = rp.delete(b) if rp.key?(b);a}
        fx = rp.delete(:filter){|k|{}}
        af = column_names + @extra_fields
        rj = rp.keys.select{|x|!af.include?(x)}; rj.each{|k|rp.delete(k)}
        fx = fx.merge(rp) if fx.is_a?(Hash)
        px = (!fx.empty? ? {filter: fx} : {}).merge(px)
        
        ([:filter, :having] & px.keys).each do |c|
          if px[c].is_a?(Hash)
            cr = px[c].keys.inject({}){|a,b|a[@field_maps.fetch(b, b)] = px[c][b];a}
            px[c] = HashResolver.new(self, cr).criteria
          end
        end
        
        SelectParams.new(px)
      elsif params.is_a?(String)
        SelectParams.new(filter: params)
      else
        SelectParams.new
      end
    end
    def criteria(params)
      if params.is_a?(Hash)
        HashResolver.new(self, params).criteria
      else
        params
      end
    end
    def update_params(params)
      if params.is_a?(Hash)
        HashResolver.new(self, params).fields_values
      else
        params
      end
    end
    def all(params = SelectParams.new)
      if params.is_a?(SelectParams)
        sp = params
      elsif params.is_a?(Hash)
        sp = self.select_params(params)
      elsif params.is_a?(String) && !params.empty?
        sp = SelectParams.new(filter: params)
      else
        raise "#{self.class}.all => parameter must be a SelectParams, Hash, or non-empty String, #{params.class} given."
      end
      
      output self.query(self.select_query(sp))
    end
    def load(params = SelectParams.new)
      if params.is_a?(SelectParams)
        sp = params
      elsif params.is_a?(Hash)
        sp = self.select_params(params)
      elsif params.is_a?(String) && !params.empty?
        sp = SelectParams.new(filter: params)
      else
        raise "#{self.class}.load => parameter must be a SelectParams, Hash, or non-empty String, #{params.class} given."
      end
      
      sp.limit = 1
      
      if !(rr = self.query(select_query(sp))).nil?
        output(rr).first
      else
        nil
      end
    end
    def exist?(criteria)
      !load(criteria).nil?
    end
    def scalar(t)
      #if !(rs = self.query(t)).nil?
      #  rs.first[rs.first.keys[0]]
      #else
      #  nil
      #end
      @connection.scalar(t)
    end
    def insert(params)
      hr = HashResolver.new(self, params)
      qs = "INSERT INTO `#{@table_name}`(#{hr.fields}) VALUES(#{hr.values})"
      self.query(qs)
      @last_id = @connection.last_id
    end
    def update(params, criteria, limit = 0)
      if !(vv = update_params(params)).is_a?(String)
        raise "#{self.class}.update => params must be a Hash or String, #{params.class} given."
      elsif !(cc = self.criteria(criteria)).is_a?(String)
        raise "#{self.class}.update => criteria must be Hash or String, #{criteria.class} given."
      else
        cc = !cc.empty? ? " WHERE #{cc}" : ""
        lc = limit > 0 ? " LIMIT #{limit}" : ""
        qs = "UPDATE `#{@table_name}` SET #{vv}#{cc}#{lc}"
        self.query(qs)
      end
    end
    def delete(criteria, limit = 0)
      if !criteria.is_a?(String)
        raise "#{self.class}.delete => criteria must be a String, #{criteria.class} given."
      else
        cc = criteria.strip
        lc = limit > 0 ? " LIMIT #{limit}" : ""
        qs = "DELETE FROM `#{@table_name}` WHERE #{cc}#{lc}"
        self.query(qs)
      end
    end
    def truncate
      self.query "TRUNCATE `#{@table_name}`"
    end
  end
end
