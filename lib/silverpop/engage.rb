require 'active_support/core_ext/object/blank'
require 'ostruct'

module Silverpop
  class Engage < Silverpop::Base
    class << self
      attr_accessor :url, :username, :password
      attr_accessor :ftp_url, :ftp_port, :ftp_username, :ftp_password
      attr_accessor :verbose
    end

    def initialize
      @session_id = nil
      @session_encoding = nil
      @response_xml = nil
      @verbose = false
    end

    ###
    #   QUERY AND SERVER RESPONSE
    ###

    SilverpopEngageError = Class.new(RuntimeError)
    def query(xml, fail_on_error: true)
      puts xml if verbose
      (@response_xml = super(xml, @session_encoding.to_s)).tap do
        puts @response_xml if verbose
        raise SilverpopEngageError, error_message unless success? || !fail_on_error
      end
    end

    def success?
      return false if @response_xml.blank?
      doc = Nokogiri::XML(@response_xml)
      doc.at('SUCCESS').text.upcase == 'TRUE'
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
        @session_id = doc.at('SESSIONID').text
        @session_encoding = doc.at('SESSION_ENCODING').text
      end
      success?
    end

    def logout
      return false unless logged_in?
      query(xml_logout)
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
    def get_lists(visibility, list_type, folder_id = nil)
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
      query(xml_get_lists(visibility, list_type, folder_id))
    end

    def get_list(id, fields)
      query(xml_export_list(id, fields))
    end

    def get_list_metadata(list_id)
      query(xml_wrapper { "<GetListMetaData><LIST_ID>#{list_id}</LIST_ID></GetListMetaData>" })
    end

    def create_contact_list(database_id:, name:, visibility: 1, parent_folder_id: nil, parent_folder_path: nil)
      query(xml_create_contact_list(database_id, name, visibility, parent_folder_id, parent_folder_path))
    end

    def calculate_query(query_id, email = nil)
      query(xml_calculate_query(query_id, email))
    end

    def import_list(map_file_path, source_file_path)
      Net::FTP.open(ftp_url) do |ftp|
        ftp.passive = true  # IMPORTANT! SILVERPOP NEEDS THIS OR IT ACTS WEIRD.
        ftp.login(ftp_username, ftp_password)
        ftp.chdir('upload')
        ftp.puttextfile(map_file_path)
        ftp.puttextfile(source_file_path)
      end

      response_xml = query(xml_import_list(File.basename(map_file_path), File.basename(source_file_path)))
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
      response = query(xml_raw_recipient_data_export(options))
      doc = Nokogiri::XML(response)
      file_name = doc.at('FILE_PATH').text
      job_id = doc.at('JOB_ID').text
      on_job_ready(job_id) { download_from_ftp(file_name, destination_file) }
      file_name
    end

    def export_list(id, fields, destination_file)
      xml = get_list(id, fields)
      doc = Nokogiri::XML(xml)
      file_name = doc.at('FILE_PATH').text
      job_id = doc.at('JOB_ID').text
      on_job_ready(job_id) { download_from_ftp(file_name, destination_file) }
    end

    def import_table(map_file_path, source_file_path)
      Net::FTP.open(ftp_url) do |ftp|
        ftp.passive = true  # IMPORTANT! SILVERPOP NEEDS THIS OR IT ACTS WEIRD.
        ftp.login(ftp_username, ftp_password)
        ftp.chdir('upload')
        ftp.puttextfile(map_file_path)
        ftp.puttextfile(source_file_path)
      end
      query(xml_import_table(File.basename(map_file_path), File.basename(source_file_path)))
    end

    def create_map_file(path:, list_info:, columns: [], mappings:, contact_lists: [], type: "LIST")
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
      File.open(path, 'w') do |file|
        file.puts(xml_map_file(list_info, columns, mappings, contact_lists, type))
      end
      path
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
      query(xml_add_recipient(list_id, email, extra_columns, created_from))
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
      query(xml_update_recipient(list_id, old_email, new_email, extra_columns, created_from))
    end

    def remove_recipient(list_id, email)
      query(xml_remove_recipient(list_id, email))
    end

    def double_opt_in_recipient(list_id, email, extra_columns=[])
      query(xml_double_opt_in_recipient(list_id, email, extra_columns))
    end

    def opt_out_recipient(list_id, email)
      query(xml_opt_out_recipient(list_id, email))
    end

    def insert_update_relational_data(table_id, data)
      query(xml_insert_update_relational_data(table_id, data))
    end

    def create_relational_table(schema)
      query(xml_create_relational_table(schema))
    end

    def associate_relational_table(list_id, table_id, field_mappings)
      query(xml_associate_relational_table(list_id, table_id, field_mappings))
    end

    # Column is a hash with :name, :type, optional :default value,
    # and optional :selection_values (for 'select one' & 'multi-select' columns).
    def add_list_column(list_id:, column:)
      query(xml_add_list_column(list_id, column))
    end

    ###
    #   MANAGE MAILINGS
    ###
    def save_mailing(header:, html_body:, aol_body: nil, text_body: nil, click_throughs: [])
      query(xml_save_mailing(header, html_body, aol_body, text_body, click_throughs))
    end

    # Currently only supports using existing template content in the top-level folder.
    # Consult API documentation and tweak XML to handle those additional options.
    def schedule_mailing(template_id:, list_id:, name:, use_html: true, visibility: 1, suppression_list_ids: [], custom_opt_out: nil)
      xml = query(xml_schedule_mailing(template_id, list_id, name, use_html, visibility, suppression_list_ids, custom_opt_out))
      Nokogiri::XML(xml).at('MAILING_ID').text
    end

    def get_mailing_templates(visibility: 1)
      query(xml_get_mailing_templates(visibility))
    end

    def list_recipient_mailings(list_id:, recipient_id:)
      query(xml_list_recipient_mailings(list_id, recipient_id))
    end

    def get_sent_mailings_for_list(list_id:, start_date:, end_date:, options: {})
      start_date = start_date.strftime('%m/%d/%Y %H:%M:%S')
      end_date = end_date.strftime('%m/%d/%Y %H:%M:%S')
      query(xml_get_sent_mailings_for_list(list_id, start_date, end_date, options))
    end

  ###
  #   API XML TEMPLATES
  ###
  protected

    # Some API calls want a number, some want a name.
    COLUMN_TYPES = {
      "TEXT" => 0,
      "YESNO" => 1,
      "NUMERIC" => 2,
      "DATE" => 3,
      "TIME" => 4,
      "COUNTRY" => 5,
      "SELECTION" => 6,
      "SEGMENTING" => 8,
      "EMAIL" => 9
    }

    def xml_login(username, password)
      xml_wrapper do
        <<-XML
          <Login>
            <USERNAME>#{username}</USERNAME>
            <PASSWORD>#{password}</PASSWORD>
          </Login>
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

    def xml_get_lists(visibility, list_type, folder_id)
      xml_wrapper do
        <<-XML
          <GetLists>
            <VISIBILITY>#{visibility}</VISIBILITY>
            <LIST_TYPE>#{list_type}</LIST_TYPE>
            #{"<FOLDER_ID>#{folder_id}</FOLDER_ID>" if folder_id}
          </GetLists>
        XML
      end
    end

    def xml_export_list(id, fields, add_to_stored_files: false)
      columns = fields.map { |f| "<COLUMN>#{f}</COLUMN>" }.join
      xml_wrapper do
        <<-XML
          <ExportList>
            <LIST_ID>#{id}</LIST_ID>
            <EXPORT_TYPE>ALL</EXPORT_TYPE>
            <EXPORT_FORMAT>CSV</EXPORT_FORMAT>
            #{add_to_stored_files ? '<ADD_TO_STORED_FILES/>' : ''}
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

    def xml_map_file(list_info, columns, mappings, contact_lists, type="LIST")
      <<-XML
        <#{type}_IMPORT>
          <#{type}_INFO>
            #{xml_map_file_list_info(list_info, type)}
          </#{type}_INFO>

          #{columns.any? ? "<COLUMNS>" : nil}
            #{columns.map { |c| xml_map_file_column(c) }.join}
          #{columns.any? ? "</COLUMNS>" : nil}

          <MAPPING>
            #{mappings.map { |m| xml_map_file_mapping_column(m) }.join}
          </MAPPING>

          #{contact_lists.any? ? "<CONTACT_LISTS>" : nil }
            #{contact_lists.map { |id| "<CONTACT_LIST_ID>#{id}</CONTACT_LIST_ID>" }.join}
          #{contact_lists.any? ? "</CONTACT_LISTS>" : nil }
        </#{type}_IMPORT>
      XML
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
      <<-XML
        <ACTION>#{list_info[:action]}</ACTION>
        #{"<#{type}_NAME>#{list_info[:list_name]}</#{type}_NAME>" if list_info[:list_name]}
        <#{type}_ID>#{list_info[:list_id]}</#{type}_ID>
        <LIST_TYPE>#{list_info[:list_type]}</LIST_TYPE>
        <FILE_TYPE>#{list_info[:file_type]}</FILE_TYPE>
        <HASHEADERS>#{list_info[:has_headers]}</HASHEADERS>
        <#{type}_VISIBILITY>#{list_info[:list_visibility]}</#{type}_VISIBILITY>
        #{xml_sync_fields(list_info[:sync_fields])}
      XML
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
      #
      # KEY_COLUMN
      #   Added to field definition and defines a field as a unique key for the
      #   list when set to True. You can define more than one unique field for
      #   each list.
      <<-XML
        <COLUMN>
          <NAME>#{column[:name].upcase}</NAME>
          <TYPE>#{column[:type]}</TYPE>
          <IS_REQUIRED>#{column[:is_required]}</IS_REQUIRED>
          <KEY_COLUMN>#{column[:key_column]}</KEY_COLUMN>
        </COLUMN>
      XML
    end

    def xml_map_file_mapping_column(column)
      <<-XML
        <COLUMN>
          <INDEX>#{column[:index]}</INDEX>
          <NAME>#{column[:name].upcase}</NAME>
          <INCLUDE>#{column[:include].nil? ? column[:include] : true}</INCLUDE>
        </COLUMN>
      XML
    end

    def xml_add_recipient(list_id, email, extra_columns, created_from)
      xml_wrapper do
        <<-XML
          <AddRecipient>
            <LIST_ID>#{list_id}</LIST_ID>
            <CREATED_FROM>#{created_from}</CREATED_FROM>
            <UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>
            <COLUMN>
              <NAME>EMAIL</NAME>
              <VALUE>#{email}</VALUE>
            </COLUMN>
            #{extra_columns.map { |c| xml_add_recipient_column(c[:name], c[:value]) }.join}
          </AddRecipient>
        XML
      end
    end

    def xml_update_recipient(list_id, old_email, new_email, extra_columns, created_from)
      xml_wrapper do
        <<-XML
          <UpdateRecipient>
            <LIST_ID>#{list_id}</LIST_ID>
            <CREATED_FROM>#{created_from}</CREATED_FROM>
            <OLD_EMAIL>#{old_email}</OLD_EMAIL>
            <COLUMN>
              <NAME>EMAIL</NAME>
              <VALUE>#{new_email}</VALUE>
            </COLUMN>
            #{extra_columns.map { |c| xml_add_recipient_column(c[:name], c[:value]) }.join}
          </UpdateRecipient>
        XML
      end
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
      xml_wrapper do
        <<-XML
          <DoubleOptInRecipient>
            <LIST_ID>#{list_id}</LIST_ID>
              <COLUMN>
                <NAME>EMAIL</NAME>
                <VALUE>#{email}</VALUE>
              </COLUMN>
          </DoubleOptInRecipient>
        XML
      end
    end

    def xml_opt_out_recipient(list_id, email)
      xml_wrapper do
        <<-XML
          <OptOutRecipient>
            <LIST_ID>#{list_id}</LIST_ID>
            <EMAIL>#{email}</EMAIL>
          </OptOutRecipient>
        XML
      end
    end

    def xml_insert_update_relational_data(table_id, data)
      xml_wrapper do
        <<-XML
          <InsertUpdateRelationalTable>
            <TABLE_ID>#{table_id}</TABLE_ID>
            <ROWS>#{xml_add_relational_rows(data)}</ROWS>
          </InsertUpdateRelationalTable>
        XML
      end
    end

    def xml_add_relational_rows(data)
      data.map { |row| "<ROW>#{xml_add_relational_row(row).join}</ROW>" }.join
    end

    def xml_add_relational_row(row_data)
      row_data.map do |column|
        "<COLUMN name=\"#{column[:name]}\"><![CDATA[#{column[:value]}]]></COLUMN>"
      end
    end

    def xml_create_relational_table(schema)
      columns = schema[:columns].map { |c| xml_add_relational_table_column(c) }.join
      xml_wrapper do
        <<-XML
          <CreateTable>
            <TABLE_NAME>#{schema[:table_name]}</TABLE_NAME>
            <COLUMNS>#{columns}</COLUMNS>
          </CreateTable>
        XML
      end
    end

    def xml_add_relational_table_column(col)
      <<-XML
        <COLUMN>
          #{"<NAME>#{col[:name]}</NAME>" if col[:name]}
          #{"<TYPE>#{col[:type]}</TYPE>" if col[:type]}
          #{"<IS_REQUIRED>#{col[:is_required]}</IS_REQUIRED>" if col[:is_required]}
          #{"<KEY_COLUMN>#{col[:key_column]}</KEY_COLUMN>" if col[:key_column]}
        </COLUMN>
      XML
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

    def xml_raw_recipient_data_export(options)
      xml_fields = options.map do |field, value|
        if field == :columns
          <<-XML
            <COLUMNS>
              #{options.columns.map { |c| "<COLUMN><NAME>#{c}</NAME></COLUMN>" }.join}
            </COLUMNS>
          XML
        elsif field == :mailing_ids
          if value.length == 1
            "<MAILING_ID>#{value.first}</MAILING_ID>"
          elsif value.length > 1
            value.map { |id| "<MAILING><MAILING_ID>#{id}</MAILING_ID></MAILING>"}.join
          else
            ''
          end
        elsif field.is_a?(Symbol)
          if value == true
            "<#{field.upcase}/>"
          else
            "<#{field.upcase}>#{value}</#{field.upcase}>"
          end
        else
          raise ArgumentError, "Field '#{field}' must be a symbol (#{field.class})."
        end
      end
      xml_wrapper { "<RawRecipientDataExport>#{xml_fields}</RawRecipientDataExport>" }
    end

    # Saves a new or updating an existing mailing template that may be used against a
    # Database, Contact List or Query.  This will replace any existing template with the
    # same MailingName element, and will update any existing template specified with a MailingId.
    def xml_save_mailing(header, html_body, aol_body, text_body, click_throughs)
      xml_wrapper do
        <<-XML
          <SaveMailing>
            <Header>
              #{"<MailingId>#{header[:id]}</MailingId>" if header[:id]}
              <MailingName><![CDATA[#{strip_cdata(header[:name])}]]></MailingName>
              <Subject><![CDATA[#{strip_cdata(header[:subject])}]]></Subject>
              <ListID>#{header[:list_id]}</ListID>
              <FromName><![CDATA[#{strip_cdata(header[:from_name])}]]></FromName>
              <FromAddress><![CDATA[#{strip_cdata(header[:from_address])}]]></FromAddress>
              <ReplyTo>#{header[:reply_to]}</ReplyTo>
              <Visibility>#{header[:visibility] || 1}</Visibility>
              <Encoding>#{header[:encoding] || 0}</Encoding>
              <TrackingLevel>#{header[:tracking_level] || 4}</TrackingLevel>
              #{"<FolderPath>#{header[:folder_path]}</FolderPath>" if header[:folder_path]}
              #{"<ClickHereMessage/>" if header[:click_here_message]}
              #{"<IsCrmTemplate>#{header[:is_crm_template]}</IsCrmTemplate>" unless header[:is_crm_template].nil?}
              #{"<HasSpCrmBlock>#{header[:has_sp_crm_block]}</HasSpCrmBlock>" unless header[:has_sp_crm_block].nil?}
              #{"<PersonalFromName>#{header[:personal_from_name]}</PersonalFromName>" if header[:personal_from_name]}
              #{"<PersonalFromAddress>#{header[:personal_from_address]}</PersonalFromAddress>" if header[:personal_from_address]}
              #{"<PersonalReplyTo>#{header[:personal_reply_to]}</PersonalReplyTo>" if header[:personal_reply_to]}
            </Header>
            <MessageBodies>
              <HTMLBody><![CDATA[#{strip_cdata(html_body)}]]></HTMLBody>
              #{"<AOLBody><![CDATA[#{strip_cdata(aol_body)}]]></AOLBody>" if aol_body}
              #{"<TextBody><![CDATA[#{strip_cdata(text_body)}]]></TextBody>" if text_body}
            </MessageBodies>
            <ClickThroughs>#{xml_click_throughs(click_throughs).join}</ClickThroughs>
            <ForwardToFriend>
              <ForwardType>0</ForwardType>
            </ForwardToFriend>
          </SaveMailing>
        XML
      end
    end

    def xml_click_throughs(click_throughs)
      click_throughs.map do |ct|
        <<-XML
          <ClickThroughName>#{ct[:name]}</ClickThroughName>
          <ClickThroughURL>#{ct[:url]}</ClickThroughURL>
          <ClickThroughType>#{ct[:type] || 2}</ClickThroughType>
        XML
      end
    end

    def xml_create_contact_list(database_id, name, visibility, parent_folder_id, parent_folder_path)
      xml_wrapper do
        <<-XML
          <CreateContactList>
            <DATABASE_ID>#{database_id}</DATABASE_ID>
            <CONTACT_LIST_NAME>#{name}</CONTACT_LIST_NAME>
            <VISIBILITY>#{visibility}</VISIBILITY>
            #{"<PARENT_FOLDER_ID>#{parent_folder_id}</PARENT_FOLDER_ID>" if parent_folder_id}
            #{"<PARENT_FOLDER_PATH>#{parent_folder_path}</PARENT_FOLDER_PATH>" if parent_folder_path}
          </CreateContactList>
        XML
      end
    end

    def xml_sync_fields(field_names)
      "<SYNC_FIELDS>#{field_names.map { |n| "<SYNC_FIELD><NAME>#{n}</NAME></SYNC_FIELD>" }.join}</SYNC_FIELDS>"
    end

    def xml_schedule_mailing(template_id, list_id, name, use_html, visibility, suppression_list_ids, custom_opt_out)
      sids = suppression_list_ids.map do |id|
        "<SUPPRESSION_LIST_ID>#{id}</SUPPRESSION_LIST_ID>"
      end

      xml_wrapper do
        <<-XML
          <ScheduleMailing>
            <TEMPLATE_ID>#{template_id}</TEMPLATE_ID>
            <LIST_ID>#{list_id}</LIST_ID>
            <MAILING_NAME>#{name}</MAILING_NAME>
            #{use_html ? "<SEND_HTML/><SEND_TEXT/>": "<SEND_TEXT/>"}
            #{sids.any? ? "<SUPPRESSION_LISTS>#{sids.join}</SUPPRESSION_LISTS>" : nil}
            #{custom_opt_out ? "<CUSTOM_OPT_OUT>#{custom_opt_out}</CUSTOM_OPT_OUT>" : nil}
            <VISIBILITY>#{visibility}</VISIBILITY>
          </ScheduleMailing>
        XML
      end
    end

    def xml_get_mailing_templates(visibility)
      xml_wrapper do
        "<GetMailingTemplates><VISIBILITY>#{visibility}</VISIBILITY></GetMailingTemplates>"
      end
    end

    def xml_list_recipient_mailings(list_id, recipient_id)
      xml_wrapper do
        <<-XML
          <ListRecipientMailings>
            <LIST_ID>#{list_id}</LIST_ID>
            #{"<RECIPIENT_ID>#{recipient_id}</RECIPIENT_ID>" if recipient_id}
          </ListRecipientMailings>
        XML
      end
    end

    def xml_get_sent_mailings_for_list(list_id, start_date, end_date, opts)
      xml_wrapper do
        required_xml = <<-XML
          <GetSentMailingsForList>
            <LIST_ID>#{list_id}</LIST_ID>
            <DATE_START>#{start_date}</DATE_START>
            <DATE_END>#{end_date}</DATE_END>
        XML
        optional_xml = opts.keys.each_with_object([]) do |key, arr|
          arr << "<#{key.upcase}/>" if opts[key]
        end
        required_xml + optional_xml.join + "</GetSentMailingsForList>"
      end
    end

    def xml_add_list_column(list_id, column)
      selection_values = (column[:select_values] || []).map { |v| "<VALUE>#{v}</VALUE>"}
      xml_wrapper do
        <<-XML
          <AddListColumn>
            <LIST_ID>#{list_id}</LIST_ID>
            <COLUMN_NAME>#{column[:name]}</COLUMN_NAME>
            <COLUMN_TYPE>#{column[:type]}</COLUMN_TYPE>
            #{column[:default] ? "<DEFAULT>#{column[:default]}</DEFAULT>" : ''}
            #{selection_values.any? ? "<SELECTION_VALUES>#{selection_values.join}</SELECTION_VALUES>" : ''}
          </AddListColumn>
        XML
      end
    end

    # Wraps the result of the block in envelope and body tags.
    def xml_wrapper(&block)
      "<Envelope><Body>#{block.call}</Body></Envelope>"
    end

    def download_from_ftp(file_name, destination_file)
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
  end
end
