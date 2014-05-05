require 'mqtt'
require 'yaml'
require 'json'
require 'virtus'


class Message
  class Base
    include Virtus.model

    attribute :payload, String
    attribute :created_at, DateTime

    def type
      self.class.name.to_s
    end

    def to_json
      attributes.to_json
    end
  end

  class Addressable < Base
    attribute :message_id, String
    attribute :reply_queue, String
  end

  class Reply < Base
    attribute :original_message_id, String
    attribute :node_type, String
    attribute :node_id, String
  end

  class ServiceDirectory
    class Ping < Message::Addressable
    end

    class Pong < Message::Reply
    end

    class Query < Message::Addressable
    end
  end
end


configurations = {}

MQTT::Client.connect('localhost') do |c|
  c.get('directory/services') do |topic,message|
    begin
      parsed_message = JSON.parse(message, symbolize_names: true)

      puts "Processing: #{parsed_message}"

      case parsed_message[:type]
      when "Message::ServiceDirectory::Ping"
        puts "Publishing reply to #{parsed_message[:reply_queue]}"
        c.publish(parsed_message[:reply_queue], Message::ServiceDirectory::Pong.new( original_message_id: parsed_message[:message_id], node_type: 'ConfigurationMananger', node_id: Process.pid).to_json )
      when "Message::ServiceDirectory::Query"
        puts "Publishing reply to #{parsed_message[:reply_queue]}"
        c.publish(parsed_message[:reply_queue], "NOT IMPLEMENTED YET")
      else
        puts "Ignoring unsupported message"
      end

    rescue StandardError => e
      puts "Caught: #{e.inspect}"
      puts "When processing message in #{topic} topic with the following payload: #{message}"
    end
  end
end


# "{\"type\": \"Message::ServiceDirectory::Ping", \"message_id\":10298131,\"reply_queue\":\"testing\"}"
