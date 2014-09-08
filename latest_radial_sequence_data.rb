require 'rubygems'
require 'net/http'
require 'net/https'
require 'uri'
require 'keen'
require 'json'
require 'date'
require 'time'
require 'active_support/all' # for datetime calculation e.g. weeks.ago.at_beginning_of_week
require 'cgi' # for URL encoding
require 'yaml' # for super secret keys in settings.yml
require 'set'

# Save your keys in `settings.yml`.
SETTINGS = YAML.load_file('settings.yml')

# Save your collection name (wich will show up as an event collection in Keen IO)
# and the number of events you'd like to analyze in `settings.yml`.
COLLECTION_NAME = SETTINGS['collection_name']

NUM_LATEST_EVENTS = SETTINGS['num_latest_events']

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

# Extract from Keen IO a bunch of sessions IDs that we would like study.
completed_sessions = keen_project.extraction(
    'session_end',
    :target_property => 'session.id',
    :latest => NUM_LATEST_EVENTS,
)

# Confirm that you're looking at the number of events you're expecting to look at.
puts completed_sessions.length

# Now let's define the maximum number of steps shown in a given flow.
maxlength = 8

# Each flow starts with a 'session_start', but beyond that, the following events
# can occur in varying orders (aka flows).
completed_sessions.each do |data|

    puts session_id = data['session']['id']

    # Create a bucket to store all the session events for each session
    # Start by filling it with `screen_view` events.
    session_events = keen_project.extraction('screen_views',
        :filters => [{
            :property_name => 'session.id',
            :operator => 'eq',
            :property_value => session_id,
        }],
        # This `screen_view` collection happens to have a label for each screen called `session_step`.
        :property_names => (['session_step', 'keen.timestamp']).to_json
    )

    # Now go get other types of events, such as payment events.
    payment = keen_project.extraction('payment',
        :filters => [{
            :property_name => 'session.id',
            :operator => 'eq',
            :property_value => session_id,
        }],
        :property_names => (['event.amount_dollars', 'keen.timestamp']).to_json
    )

    # Add the payment events to the array of events.
        payment.each do |m|
            # To match the existing events we have in session_events, we're adding new
            # `session_step` events with the label 'money'.
            m['session_step'] = 'money'
        session_events << m
        end
    end

    # Add play events to the set of session events.
    plays = keen_project.extraction('play',
        :filters => [{
            :property_name => 'session.id',
            :operator => 'eq',
            :property_value => session_id,
        }],
        :property_names => (['event.play_type', 'keen.timestamp']).to_json
    )

    unless plays.empty?
        plays.each do |p|
            # Add `session_step` events with the label 'plays'.
            p['session_step'] = 'play'
        session_events << p
        end
    end

    puts '_____________'

    # Now let's convert the timestamps we get from Keen IO into Epoch integers.
    session_events.each do |e|
        e['keen']['timestamp_epoch'] = DateTime.parse(e['keen']['timestamp']).to_time.to_i
    end

    # Sort the events by their timestamps so that we can see them in the order they happened
    sorted_events = session_events.sort_by { |hsh| hsh['keen']['timestamp_epoch']}

    flow = ''

    # We'll use `count` to check if a flow has reached the maximum number of events.
    count = 0

    previous_event = nil

    sorted_events.each.with_index(0) do |event, i|

        # Add events to flows.
        flow = flow + event['session_step'] + '-'
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

        previous_event = event['session_step']

    end

    puts timestamp = sorted_events[0]['keen']['timestamp']
    puts flow

    # Finally, we publish the user flows to an event collection in Keen IO.
    puts keen_output.publish(COLLECTION_NAME, {
        :keen => {:timestamp => timestamp},
        :flows => flow,
    })
end
