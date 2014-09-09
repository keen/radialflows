require 'rubygems'
require 'net/http'
require 'net/https'
require 'uri'
require 'keen'
require 'json'
require 'date'
require 'time'
require 'active_support/all' # for datetime calculation e.g. weeks.ago.at_beginning_of_week
require 'simple_xlsx' # for outputting excel files
require 'cgi' # for URL encoding
require 'yaml' # for super secret keys in settings.yml

# Set up your keys in `settings.yml`
SETTINGS = YAML.load_file('settings.yml')

# Save your collection name (wich will show up as an event collection in Keen IO)
# and the timeframe you'd like to analyze in `settings.yml`.
TIMEFRAME = SETTINGS['timeframe']

COLLECTION_NAME = SETTINGS['collection_name']

# First, we enter the Keen IO project info for the data we'll be reading.
keen_project = Keen::Client.new(
    :project_id => SETTINGS['project_id'],
    :read_key => SETTINGS['read_key'],
    :write_key => SETTINGS['write_key'],
)

# If you're going to publish your data output to an event collection in a different
# Keen IO project, you'll need to specify that project info too.
keen_output = Keen::Client.new(
    :project_id => SETTINGS['output_project_id'],
    :read_key => SETTINGS['output_read_key'],
    :write_key => SETTINGS['output_write_key'],
)

puts 'hello world!'

# Grab all the unique sessions.
unique_session_ids = keen_project.select_unique(
    'session_start',
    :target_property => 'session.id',
    :timeframe => TIMEFRAME
)

puts unique_session_ids.length

# Let's define the maximum number of steps shown in a given flow.
maxlength = 8

# Create a bucket to store all the session events for each session.
session_events = []

unique_session_ids.each do |session_id|

    # screenviews
    screenviews = keen_project.extraction('session_end',
        :timeframe => TIMEFRAME,
        :filters => [{
            :property_name => 'session.id',
            :operator => 'eq',
            :property_value => session_id,
        }],
        :property_names => (['view_type', 'keen.timestamp']).to_json
    )

    unless screenviews.empty?
        screenviews.each do |v|
            p['screenname'] = 'view'
        session_events << v
        end
    end

    # payment
    payment = keen_project.extraction('payment',
        :timeframe => TIMEFRAME,
        :filters => [{
            :property_name => 'session.id',
            :operator => 'eq',
            :property_value => session_id,
        }],
        :property_names => (['event.amount_dollars', 'keen.timestamp']).to_json

    )

    unless payment.empty?
        payment.each do |m|
            m['screenname'] = 'money'
        session_events << m
        end
    end

    # plays
    plays = keen_project.extraction('play',
        :timeframe => TIMEFRAME,
        :filters => [{
            :property_name => 'session.id',
            :operator => 'eq',
            :property_value => session_id,
        }],
        :property_names => (['event.play_type', 'keen.timestamp']).to_json
    )

    unless plays.empty?
        plays.each do |p|
            p['screenname'] = 'play'
        session_events << p
        end
    end

    puts '_____________'

    # Now let's convert the timestamps we get from Keen IO into Epoch integers.
    session_events.each do |e|
        e['keen']['timestamp_epoch'] = DateTime.parse(e['keen']['timestamp']).to_time.to_i
    end

    sorted_events = session_events.sort_by { |hsh| hsh['keen']['timestamp_epoch']}

    flow = ''

    # We'll use `count` to check if a flow has reached the maximum number of events.
    count = 0

    previous_event = nil

    sorted_events.each.with_index(0) do |event, i|

        # Add events to flows.
        flow = flow + event['screenname'] + '-'
        count += 1

        # If the index corresponds to the length of `sorted_events`, we've reached
        # the end of the flow.
        if i == sorted_events.length - 1
            flow = flow + 'end'
            break
        end

        # If the number of events in the flow (specified by `count`) has reached our
        # designated maxlength and there are still more events in `sorted_events`
        # (specified by the flow ending in `–` instead of `end`), we'll add `...` to
        # the end of our flow.
        if count == maxlength and flow.end_with? '-' then
           flow = flow + '...'
           break
        end

        previous_event = event['screenname']

        i = i + 1

    end

    puts timestamp = sorted_events[0]['keen']['timestamp']
    puts flow

    # Finally, we publish the user flows to an event collection in Keen IO.
    puts keen_output.publish(COLLECTION_NAME, {
        :keen => {:timestamp => timestamp},
        :flows => flow,
    })
end
