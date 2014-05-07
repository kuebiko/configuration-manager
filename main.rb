require 'mqtt'
require 'yaml'
require 'json'
require 'kuebiko'

configurations = {}

MQTT::Client.connect('localhost') do |c|
  c.get('directory/services') do |topic,raw_message|
    begin
      parsed_message = Kuebiko::Message.build_from_hash( JSON.parse(raw_message, symbolize_names: true) )

      if parsed_message.is_a?(Kuebiko::Message::Command)
        case parsed_message.command
        when 'ping'
          c.publish(parsed_message.reply_queue, Kuebiko::Message::Reply.new( original_message_id: parsed_message.message_id, node_type: 'ConfigurationMananger', node_id: Process.pid).to_json )
        when 'query'
          # Lookup requested configuration
          configurations[ parsed_message.payload.downcase ] ||= YAML.load_file("config_files/#{parsed_message[:payload].downcase}.yml")

          if configurations[ parsed_message.payload.downcase ]
            c.publish(parsed_message.reply_queue, Kuebiko::Message::Reply.new( original_message_id: parsed_message.message_id, node_type: 'ConfigurationMananger', node_id: Process.pid, payload: configurations[ parsed_message.payload.downcase ].to_json ).to_json )
          end
        end
      else
        puts "Ignoring unsupported message"
      end

    rescue StandardError => e
      puts "Caught: #{e.inspect}"
      puts "When processing message in #{topic} topic with the following payload: #{message}"
    end
  end
end
