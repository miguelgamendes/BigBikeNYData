#!/usr/bin/env ruby
# Use: ruby spark2geojson.rb sparkfile > geo.js

require 'json'
ls=[]
ARGF.each_line do |l|
  ls << eval(l.to_s.strip.gsub("(","[").gsub(")","]").gsub("u'","'"))
end
r={"type"=>"FeatureCollection","features"=>
  ls.map do |l|
    s=l[0][0..1].reverse
    d=l[0][2..3].reverse
    {"type"=>"Feature",
      "geometry"=>{"type"=>"LineString","coordinates"=>[s,d]},
      "style"=>{"color"=>"#ff0000","weight"=>1,"opacity"=>0.65,"value"=>l[1]}}
  end
}
puts "var i=#{r.to_json};"
