#!/usr/bin/env ruby
require 'json'
require 'optparse'
icon = "womanIcon"
OptionParser.new do |opts|
  opts.on("-h","--help","This help") do
    puts opts
    exit 0
  end
  opts.on("-i","--icon ICON","Icon name to use") do |i|
    icon = i
  end
end.parse!
ARGF.each_line do |l|
  la = eval(l.to_s.strip.gsub("(","[").gsub(")","]").gsub("u'","'"))
  lat = la[0][0]
  lon = la[0][1]
  val = la[1]
  popup = "#{val}"
  puts "L.marker([#{lat},#{lon}],{icon:#{icon}}).addTo(map).bindPopup(#{popup.inspect});"
end
