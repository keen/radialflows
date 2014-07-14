require 'rubygems'
require 'net/http'
require 'net/https'
require 'uri'
require 'keen'
require 'json'
require 'date'
require 'time'
require 'active_support/all' #for datetime calculation e.g. weeks.ago.at_beginning_of_week
require 'simple_xlsx' #for outputting excel files
require 'cgi' #for URL encoding
require 'yaml' # for super secret keys in settings.yml

SETTINGS = YAML.load_file('settings.yml')

# Step 1 - Enter your Keen Project Info.
Keen.project_id = SETTINGS['output_project_id']
Keen.read_key = SETTINGS['output_read_key']

puts data = Keen.count('<your_collection_name>', :group_by => 'flows')

# Save the results to a csv file in a Results folder.
file = File.open('./Results/visit-sequences.csv', 'w')

data.each do |r|

    string = r['flows'] + ',' + r['result'].to_s
    file.puts string

end

file.close
