class ConfigurationsAgent < Kuebiko::Agent
  RESOURCE_TOPIC = 'resources/configurations'
  RESOURCE_CLASS = Kuebiko::MessagePayload::Query

  def initialize
    super

    dispatcher.register_message_handler(
      RESOURCE_TOPIC,
      RESOURCE_CLASS,
      method(:handle_query)
    )
    @resources = {}
  end

  def handle_query(msg)
    @resources[msg.payload.query.downcase] ||= YAML.load_file(path_for_resource(msg.payload.query)).fetch('settings')

    if @resources[msg.payload.query.downcase]
      reply_msg = msg.build_reply_message

      reply_msg.payload = Kuebiko::MessagePayload::Generic.new(body: @resources[msg.payload.query.downcase].to_json)

      dispatcher.send(reply_msg)
    end
  rescue StandardError => e
    puts "#{self.class.name}: #{e.message}"
    puts e.backtrace.inspect
  end

  protected

  def path_for_resource(resource)
    "config_files/#{resource.downcase}.yml"
  end
end
