require 'twitter/json_stream'
require 'eventmachine'

module Tweitgeist
  class TwitterStream
    def initialize(options = {})
      @options = options

      # default empty handlers
      @on_item = ->(item) {}
      @on_close = -> {}
      @on_error = ->(message) {}
      @on_failure = ->(message) {}
      @on_reconnect = ->(timeout, retries) {}
      @stream = nil
    end

    def on_item
      @on_item = lambda do |item|
        begin
          yield(item)
        rescue
          stop
          @on_failure.call("on_item exception #{$ERROR_INFO.message}, #{$ERROR_INFO.backtrace.join("\n")}")
        end
      end
    end

    def on_close
      @on_close = lambda do
        begin
          yield
        rescue
          stop
          @on_failure.call("on_close exception=#{$ERROR_INFO.inspect}\n#{$ERROR_INFO.backtrace.join("\n")}")
        end
      end
    end

    def on_error
      @on_error = lambda do |message|
        begin
          yield(message)
        rescue
          stop
          @on_failure.call("on_error exception=#{$ERROR_INFO.inspect}\n#{$ERROR_INFO.backtrace.join("\n")}")
        end
      end
    end

    def on_reconnect
      @on_reconnect = lambda do |timeout, retries|
        begin
          yield(timeout, retries)
        rescue
          stop
          @on_failure.call("on_item exception=#{$ERROR_INFO.inspect}\n#{$ERROR_INFO.backtrace.join("\n")}")
        end
      end
    end

    def on_failure
      @on_failure = lambda do |message|
        begin
          yield(message)
        rescue
          stop
          puts("on_failure exception=#{$ERROR_INFO.inspect}\n#{$ERROR_INFO.backtrace.join("\n")}")
        end
      end
    end

    def stop
      EventMachine.stop_event_loop if EventMachine.reactor_running?
    end

    def run
      EventMachine.run do
        @stream = Twitter::JSONStream.connect(@options)

        # attach callbacks to EM stream
        @stream.each_item(&@on_item)
        @stream.on_close(&@on_close)
        @stream.on_error(&@on_error)
        @stream.on_reconnect(&@on_reconnect)
        @stream.on_max_reconnects { |timeout, retries| @on_failure.call("failed after max reconnect=#{retries} using timeout=#{timeout}") }
      end
    end
  end
end
