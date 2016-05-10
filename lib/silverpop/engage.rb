require 'active_support/core_ext/object/blank'
require 'ostruct'

module Silverpop

  class Engage < Silverpop::Base

    class << self
      attr_accessor :url, :username, :password
      attr_accessor :ftp_url, :ftp_port, :ftp_username, :ftp_password
    end

    def initialize
      @session_id = nil
      @session_encoding = nil
      @response_xml = nil
    end

    ###
    #   QUERY AND SERVER RESPONSE
    ###
    def query(xml)
      (@response_xml = super(xml, @session_encoding.to_s)).tap do
        log_error unless success?
      end
    end

    def success?
      return false if @response_xml.blank?
      doc = Nokogiri::XML(@response_xml)
      doc.at('SUCCESS').text == 'true'
    end

    def error_message
      return false if success?
      doc = Nokogiri::XML(@response_xml)
      strip_cdata(doc.at('FaultString').text)
    end

    ###
    #   SESSION MANAGEMENT
    ###
    def login
      logout if @session_id || @session_encoding
      doc = Nokogiri::XML(query(xml_login(username, password)))
      if doc.at('SUCCESS').text == 'true'
        @session_id = doc.at('SESSIONID').innerHTML
        @session_encoding = doc.at('SESSION_ENCODING').innerHTML
      end
      success?
    end

    def logout
      return false unless logged_in?
      response_xml = query(xml_logout)
      if success?
        @session_id = nil
        @session_encoding = nil
      end
      success?
    end

    def logged_in?
      @session_id && @session_encoding
    end

    ###
    #   JOB MANAGEMENT
    ###
    def get_job_status(job_id)
      response_xml = query(xml_get_job_status(job_id))
      Nokogiri::XML(response_xml).at('JOB_STATUS').text
    end

    ###
    #   LIST MANAGEMENT
    ###
    def get_lists(visibility, list_type)
      # VISIBILITY
      # Required. Defines the visibility of the lists to return.
      # * 0 – Private
      # * 1 – Shared

      # LIST_TYPE
      # Defines the type of lists to return.
      # * 0 – Regular Lists
      # * 1 – Queries
      # * 2 – Both Regular Lists and Queries
      # * 5 – Test Lists
      # * 6 – Seed Lists
      # * 13 – Suppression Lists
      # * 15 – Relational Tables
      # * 18 – Contact Lists
      response_xml = query( xml_get_lists(visibility, list_type) )
    end

    def get_list(id, fields)
      response_xml = query( xml_export_list(id, fields) )
    end

    def calculate_query(query_id, email = nil)
      response_xml = query( xml_calculate_query(query_id, email) )
    end

    def import_list(map_file_path, source_file_path)
      Net::FTP.open(ftp_url) do |ftp|
        ftp.passive = true  # IMPORTANT! SILVERPOP NEEDS THIS OR IT ACTS WEIRD.
        ftp.login(ftp_username, ftp_password)
        ftp.chdir('upload')
        ftp.puttextfile(map_file_path)
        ftp.puttextfile(source_file_path)
      end

      map_file_ftp_path = File.basename(map_file_path)
      source_file_ftp_path = File.basename(source_file_path)

      response_xml = query xml_import_list(
                              File.basename(map_file_path),
                              File.basename(source_file_path) )
    end

    class RawRecipientDataOptions < OpenStruct
      def initialize
        super(:columns => [])
      end

      def fields
        instance_variable_get("@table").keys
      end

      [:fields=, :columns=].each do |method|
        define_method(method) do
          raise ArgumentError, "'#{method}' is reserverd word in RawRecipientDataOptions"
        end
      end
    end

    def raw_recipient_data_export(options, destination_file)
      xml = "<Envelope><Body><RawRecipientDataExport>"

      options.fields.each_with_object(xml) do |field, string|
        case field
          when :columns
            string << "<COLUMNS>"
            options.columns.each do |column|
              string << "<COLUMN><NAME>#{column}</NAME></COLUMN>"
            end
            string << "</COLUMNS>"
          when Symbol
            string << if (value = options.send(field)) == true
              "<#{field.upcase}/>"
            else
              "<#{field.upcase}>#{value}</#{field.upcase}>"
            end
          else
            raise ArgumentError, "#{field} didn't match any case"
        end
      end

      xml << "</RawRecipientDataExport></Body></Envelope>"

      response = query(xml)
      doc = Hpricot::XML(response)
      file_name = doc.at('FILE_PATH').innerHTML
      job_id = doc.at('JOB_ID').innerHTML

      on_job_ready(job_id) do

        # because of the net/ftp's lack we have to use Net::FTP.new construction
        ftp = Net::FTP.new

        # need for testing
        ftp_port ? ftp.connect(ftp_url, ftp_port) : ftp.connect(ftp_url)

        ftp.passive = true # IMPORTANT! SILVERPOP NEEDS THIS OR IT ACTS WEIRD.
        ftp.login(ftp_username, ftp_password)
        ftp.chdir('download')

        ftp.getbinaryfile(file_name, destination_file)

        ftp.close
      end

      self
    end

    def export_list(id, fields, destination_file)
      xml = get_list(id, fields)
      doc = Hpricot::XML(xml)
      file_name = doc.at('FILE_PATH').innerHTML
      job_id = doc.at('JOB_ID').innerHTML

      on_job_ready(job_id) do

        # because of the net/ftp's lack we have to use Net::FTP.new construction
        ftp = Net::FTP.new

        # need for testing
        ftp_port ? ftp.connect(ftp_url, ftp_port) : ftp.connect(ftp_url)

        ftp.passive = true # IMPORTANT! SILVERPOP NEEDS THIS OR IT ACTS WEIRD.
        ftp.login(ftp_username, ftp_password)
        ftp.chdir('download')

        ftp.gettextfile(file_name, destination_file)

        ftp.close
      end
    end

    def import_table(map_file_path, source_file_path)
      Net::FTP.open(ftp_url) do |ftp|
        ftp.passive = true  # IMPORTANT! SILVERPOP NEEDS THIS OR IT ACTS WEIRD.
        ftp.login(ftp_username, ftp_password)
        ftp.chdir('upload')
        ftp.puttextfile(map_file_path)
        ftp.puttextfile(source_file_path)
      end

      map_file_ftp_path = File.basename(map_file_path)
      source_file_ftp_path = File.basename(source_file_path)

      response_xml = query xml_import_table(
                              File.basename(map_file_path),
                              File.basename(source_file_path) )
    end

    def create_map_file (file_path, list_info, columns, mappings, type = "LIST")
      # SAMPLE_PARAMS:
      # list_info = { :action       => 'ADD_AND_UPDATE',
      #               :list_id      => 123456,
      #               :file_type    => 0,
      #               :has_headers  => true }
      # columns   = [ { :name=>'EMAIL', :type=>9, :is_required=>true, :key_column=>true },
      #               { :name=>'FIRST_NAME', :type=>0, :is_required=>false, :key_column=>false },
      #               { :name=>'LAST_NAME', :type=>0, :is_required=>false, :key_column=>false } ]
      # mappings  = [ { :index=>1, :name=>'EMAIL', :include=>true },
      #               { :index=>2, :name=>'FIRST_NAME', :include=>true },
      #               { :index=>3, :name=>'LAST_NAME', :include=>true } ]

      File.open(file_path, 'w') do |file|
        file.puts xml_map_file(list_info, columns, mappings, type)
      end

      file_path
    end

    ###
    #   RECIPIENT MANAGEMENT
    ###
    def add_recipient(list_id, email, extra_columns=[], created_from=1)
      # CREATED_FROM
      # Value indicating the way in which you are adding the selected recipient
      # to the system. Values include:
      # * 0 – Imported from a list
      # * 1 – Added manually
      # * 2 – Opted in
      # * 3 – Created from tracking list
      response_xml =  query(xml_add_recipient(
                        list_id, email, extra_columns, created_from) )
    end

    def update_recipient(list_id, old_email, new_email=nil, extra_columns=[], created_from=1)
      # CREATED_FROM
      # Value indicating the way in which you are adding the selected recipient
      # to the system. Values include:
      # * 0 – Imported from a list
      # * 1 – Added manually
      # * 2 – Opted in
      # * 3 – Created from tracking list
      new_email = old_email if new_email.nil?
      response_xml =  query(xml_update_recipient(
                        list_id, old_email, new_email, extra_columns, created_from) )
    end

    def remove_recipient(list_id, email)
      response_xml = query( xml_remove_recipient(list_id, email) )
    end

    def double_opt_in_recipient(list_id, email, extra_columns=[])
      response_xml = query xml_double_opt_in_recipient(list_id, email, extra_columns)
    end

    def opt_out_recipient(list_id, email)
      response_xml = query xml_opt_out_recipient(list_id, email)
    end

    def insert_update_relational_data(table_id, data)
      response_xml = query xml_insert_update_relational_data(table_id, data)
    end

    def create_relational_table(schema)
      response_xml = query xml_create_relational_table(schema)
    end

    def associate_relational_table(list_id, table_id, field_mappings)
      response_xml = query xml_associate_relational_table(list_id, table_id, field_mappings)
    end

  ###
  #   API XML TEMPLATES
  ###
  protected

    def map_type(type) # some API calls want a number, some want a name. This maps the name back to the number
      {
        "TEXT" => 0,
        "YESNO" => 1,
        "NUMERIC" => 2,
        "DATE" => 3,
        "TIME" => 4,
        "COUNTRY" => 5,
        "SELECTION" => 6,
        "SEGMENTING" => 8,
        "EMAIL" => 9
      }[type]
    end

    def log_error
      logger.debug '*** Silverpop::Engage Error: ' + error_message
    end

    def xml_login(username, password)
      xml_wrapper do
        <<-XML
          <Login>
            <USERNAME>#{username}</USERNAME>
            <PASSWORD>#{password}</PASSWORD>
          <Login>
        XML
      end
    end

    def xml_logout
      xml_wrapper { "<Logout/>" }
    end

    def xml_get_job_status(job_id)
      xml_wrapper do
        <<-XML
          <GetJobStatus>
            <JOB_ID>#{job_id}</JOB_ID>
          </GetJobStatus>
        XML
      end
    end

    def xml_get_lists(visibility, list_type)
      xml_wrapper do
        <<-XML
          <GetLists>
            <VISIBILITY>#{visibility}</VISIBILITY>
            <LIST_TYPE>#{list_type}</LIST_TYPE>
          </GetLists>
        XML
      end
    end

    def xml_export_list(id, fields)
      columns = fields.map { |f| "<COLUMN>#{f}</COLUMN>" }.join
      xml_wrapper do
        <<-XML
          <ExportList>
            <LIST_ID>#{id}</LIST_ID>
            <EXPORT_TYPE>ALL</EXPORT_TYPE>
            <EXPORT_FORMAT>CSV</EXPORT_FORMAT>
            <ADD_TO_STORED_FILES/>
            <EXPORT_COLUMNS>#{columns}</EXPORT_COLUMNS>
          </ExportList>
        XML
      end
    end

    def xml_calculate_query(query_id, email)
      xml_wrapper do
        <<-XML
          <CalculateQuery>
            <QUERY_ID>#{query_id}</QUERY_ID>
            #{"<EMAIL>#{email}</EMAIL>" if email}
          </CalculateQuery>
        XML
      end
    end

    def xml_import_list(map_file, source_file)
      xml_wrapper do
        <<-XML
          <ImportList>
            <MAP_FILE>#{map_file}</MAP_FILE>
            <SOURCE_FILE>#{source_file}</SOURCE_FILE>
          </ImportList>
        XML
      end
    end

    def xml_import_table(map_file, source_file)
      xml_wrapper do
        <<-XML
          <ImportTable>
            <MAP_FILE>#{map_file}</MAP_FILE>
            <SOURCE_FILE>#{source_file}</SOURCE_FILE>
          </ImportTable>
        XML
      end
    end

    def xml_map_file(list_info, columns, mappings, type="LIST")
      return false unless (columns.size > 0 && mappings.size > 0)

      xml = "<#{type}_IMPORT>"+
              "<#{type}_INFO></#{type}_INFO>"+
              '<COLUMNS></COLUMNS>'+
              '<MAPPING></MAPPING>'+
            "</#{type}_IMPORT>"

      doc = Hpricot::XML(xml)
      doc.at("#{type}_INFO").innerHTML = xml_map_file_list_info(list_info, type)

      str = ''
      columns.each { |c| str += xml_map_file_column(c) }
      doc.at('COLUMNS').innerHTML = str

      str = ''
      mappings.each { |m| str += xml_map_file_mapping_column(m) }
      doc.at('MAPPING').innerHTML = str

      doc.to_s
    end

    def xml_map_file_list_info(list_info, type = "LIST")
      # ACTION:
      #   Defines the type of list import you are performing. The following is a
      #   list of valid values and how interprets them:
      #   • CREATE
      #     – create a new list. If the list already exists, stop the import.
      #   • ADD_ONLY
      #     – only add new recipients to the list. Ignore existing recipients
      #       when found in the source file.
      #   • UPDATE_ONLY
      #     – only update the existing recipients in the list. Ignore recipients
      #       who exist in the source file but not in the list.
      #   • ADD_AND_UPDATE
      #     – process all recipients in the source file. If they already exist
      #       in the list, update their values. If they do not exist, create a
      #        new row in the list for the recipient.
      #   • OPT_OUT
      #     – opt out any recipient in the source file who is already in the list.
      #       Ignore recipients who exist in the source file but not the list.

      # FILE_TYPE:
      #   Defines the formatting of the source file. Supported values are:
      #   0 – CSV file, 1 – Tab-separated file, 2 – Pipe-separated file

      # HASHEADERS
      #   The HASHEADERS element is set to true if the first line in the source
      #   file contains column definitions. The List Import API does not use
      #   these headers, so if you have them, this element must be set to true
      #   so it can skip the first line.
      ( '<ACTION>%s</ACTION>'+
        "<#{type}_NAME>%s</#{type}_NAME>"+
        "<#{type}_ID>%s</#{type}_ID>"+
        '<FILE_TYPE>%s</FILE_TYPE>'+
        '<HASHEADERS>%s</HASHEADERS>'+
        "<#{type}_VISIBILITY>%s</#{type}_VISIBILITY>"
      ) % [ list_info[:action],
            list_info[:list_name],
            list_info[:list_id],
            list_info[:file_type],
            list_info[:has_headers],
            list_info[:list_visibility] ]
    end

    def xml_map_file_column(column)
      # TYPE
      #   Defines what type of column to create. The following is a list of
      #   valid values:
      #     0 – Text column
      #     1 – YES/No column
      #     2 – Numeric column
      #     3 – Date column
      #     4 – Time column
      #     5 – Country column
      #     6 – Select one
      #     8 – Segmenting
      #     9 – System (used for defining EMAIL field only)

      # KEY_COLUMN
      #   Added to field definition and defines a field as a unique key for the
      #   list when set to True. You can define more than one unique field for
      #   each list.

      ( '<COLUMN>'+
          '<NAME>%s</NAME>'+
          '<TYPE>%s</TYPE>'+
          '<IS_REQUIRED>%s</IS_REQUIRED>'+
          '<KEY_COLUMN>%s</KEY_COLUMN>'+
        '</COLUMN>'
      ) % [ column[:name].upcase,
            column[:type],
            column[:is_required],
            column[:key_column] ]
    end

    def xml_map_file_mapping_column(column)
      column = { :include => true }.merge(column)

      ( '<COLUMN>'+
          '<INDEX>%s</INDEX>'+
          '<NAME>%s</NAME>'+
          '<INCLUDE>true</INCLUDE>'+
        '</COLUMN>'
      ) % [ column[:index],
            column[:name].upcase,
            column[:include] ]
    end

    def xml_add_recipient(list_id, email, extra_columns, created_from)
      extra_columns = extra_columns.map do |c|
        xml_add_recipient_column(c[:name], c[:value])
      end
      xml = xml_wrapper do
        <<-XML
          <AddRecipient>
            <LIST_ID>#{list_id}</LIST_ID>
            <CREATED_FROM>#{created_from}</CREATED_FROM>
            <UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>
            <COLUMN>
              <NAME>EMAIL</NAME>
              <VALUE>#{email}</VALUE>
            </COLUMN>
            #{extra_columns.join}
          </AddRecipient>
        XML
      end
    end

    def xml_update_recipient(list_id, old_email, new_email, extra_columns, created_from)
      xml = ( '<Envelope><Body>'+
                '<UpdateRecipient>'+
                  '<LIST_ID>%s</LIST_ID>'+
                  '<CREATED_FROM>%s</CREATED_FROM>'+
                  '<OLD_EMAIL>%s</OLD_EMAIL>'+
                  '<COLUMN>'+
                    '<NAME>EMAIL</NAME>'+
                    '<VALUE>%s</VALUE>'+
                  '</COLUMN>'+
                '</UpdateRecipient>'+
              '</Body></Envelope>'
      ) % [list_id, created_from, old_email, new_email]

      doc = Hpricot::XML(xml)
      if extra_columns.size > 0
        extra_columns.each do |c|
          (doc/:UpdateRecipient).append xml_add_recipient_column(c[:name], c[:value])
        end
      end

      doc.to_s
    end

    def xml_add_recipient_column(name, value)
      "<COLUMN><NAME>#{name}</NAME><VALUE>#{value}</VALUE></COLUMN>"
    end

    def xml_remove_recipient(list_id, email)
      xml_wrapper do
        <<-XML
          <RemoveRecipient>
            <LIST_ID>#{list_id}</LIST_ID>
            <EMAIL>#{email}</EMAIL>
          </RemoveRecipient>
        XML
      end
    end

    def xml_double_opt_in_recipient(list_id, email, extra_columns)
      ( '<Envelope><Body>'+
          '<DoubleOptInRecipient>'+
            '<LIST_ID>%s</LIST_ID>'+
              '<COLUMN>'+
                '<NAME>EMAIL</NAME>'+
                '<VALUE>%s</VALUE>'+
              '</COLUMN>'+
          '</DoubleOptInRecipient>'+
        '</Body></Envelope>'
      ) % [list_id, email]
    end

    def xml_opt_out_recipient(list_id, email)
      ( '<Envelope><Body>'+
          '<OptOutRecipient>'+
            '<LIST_ID>%s</LIST_ID>'+
            '<EMAIL>%s</EMAIL>'+
          '</OptOutRecipient>'+
        '</Body></Envelope>'
      ) % [list_id, email]
    end

    def xml_insert_update_relational_data(table_id, data)
      ( '<Envelope><Body>'+
          '<InsertUpdateRelationalTable>'+
            '<TABLE_ID>%s</TABLE_ID>'+
            '<ROWS>%s</ROWS>'+
          '</InsertUpdateRelationalTable>'+
        '</Body></Envelope>'
      ) % [table_id, xml_add_relational_rows(data)]
    end

    def xml_add_relational_rows(data)
      rows = ''
      data.each do |row|
        row = ('<ROW>'+
          '%s'+
          '</ROW>'
        ) % xml_add_relational_row(row)
        rows << row
      end
      rows
    end

    def xml_add_relational_row(row_data)
      row = ''
      row_data.each do |column|
        col = ( '<COLUMN name="%s">'+
            '<![CDATA[%s]]>'+
          '</COLUMN>'
        ) % [column[:name], column[:value]]
        row << col
      end
      row
    end

    def xml_create_relational_table(schema)
      xml = ('<Envelope><Body>'+
        '<CreateTable>'+
          '<TABLE_NAME>%s</TABLE_NAME>'+
          '<COLUMNS></COLUMNS>'+
        '</CreateTable>'+
      '</Body></Envelope>') % [schema[:table_name]]

      doc = Hpricot::XML(xml)
      if schema[:columns].size > 0
        schema[:columns].each do |c|
          element = doc/:COLUMNS
          if element.innerHTML.empty?
            (doc/:COLUMNS).innerHTML= xml_add_relational_table_column(c)
          else
            (doc/:COLUMNS).append xml_add_relational_table_column(c)
          end
        end
      end

      doc.to_s
    end

    def xml_add_relational_table_column(col)
      xml = "<COLUMN>"
      xml << "<NAME>%s</NAME>" % [col[:name]] if col[:name]
      xml << "<TYPE>%s</TYPE>" % [col[:type]] if col[:type]
      xml << "<IS_REQUIRED>%s</IS_REQUIRED>" % [col[:is_required]] if col[:is_required]
      xml << "<KEY_COLUMN>%s</KEY_COLUMN>" % [col[:key_column]] if col[:key_column]
      xml << "</COLUMN>"
    end

    def xml_associate_relational_table(list_id, table_id, field_mappings)
      mappings = field_mappings.map { |m| xml_add_relational_table_mapping(m) }
      xml_wrapper do
        <<-XML
          <JoinTable>
            <TABLE_ID>#{table_id}</TABLE_ID>
            <LIST_ID>#{list_id}</LIST_ID>
            #{mappings.join}
            </JoinTable>
        XML
      end
    end

    def xml_add_relational_table_mapping(mapping)
      <<-XML
        <MAP_FIELD>
          <LIST_FIELD>#{mapping[:list_name]}</LIST_FIELD>
          <TABLE_FIELD>#{mapping[:table_name]}</TABLE_FIELD>
        </MAP_FIELD>
      XML
    end

    # Wraps the result of the block in envelope and body tags.
    def xml_wrapper(&block)
      "<Envelope><Body>#{block.call}</Body></Envelope>"
    end
  end
end
