require 'rubygems'
require 'bundler/setup'
require 'pg'
require 'faker'
require 'ostruct'
require 'byebug'

conn = PG.connect('dbname=fake_messaging_app')

# Create users
conn.prepare('insert_users', 'insert into users (name) values ($1)', &:check)

1000.times do
  conn.exec_prepared('insert_users', [Faker::Name.name], &:check)
end

# Obtain a base user
baseuser = conn.exec('select * from users limit 1') do |result|
  result.check
  OpenStruct.new(id: result.first['id'], name: result.first['name'])
end

puts "Our base user #{baseuser}"

# Create groups for that user
conn.prepare('insert_users_groups',
             'insert into users_groups (group_id, member_id) values ($1, $2)', &:check)

# Let's create 20 groups, each group size will be exponential
20.times do |group_index|
  group_id = group_index + 1
  group_size = group_id**2

  random_members = conn.exec('select id from users where id <> $1 order by random() limit $2',
                             [baseuser.id, group_size]) do |result|
    result.map { |row| row['id'] }
  end

  # insert our base user
  conn.exec_prepared('insert_users_groups', [group_id, baseuser.id], &:check)
  random_members.each do |id|
    # now add extra users
    begin
      conn.exec_prepared('insert_users_groups', [group_id, id], &:check)
    rescue PG::UniqueViolation
    end
  end
end

# let's display the created group sizes
group_ids = conn.exec('select group_id as g, count(*) as c from users_groups group by 1 order by 1') do |result|
  result.map do |row|
    puts format('%i, %i', row['g'], row['c'])
    row['g']
  end
end

# now let's add messages
conn.prepare('insert_messages',
             'insert into messages (user_id, group_id, message, created_at) values ($1, $2, $3, $4)', &:check)

# let's add a fijed number of messages per group member
window = (0..5).cycle
# All conversations mysteriously start this year on January the first
timestamps = lambda do |window|
  Array.new(10) do |index|
    Time.new(Time.now.year, 1, 1 + index, 0, 0, (0 + index ) * window, '+06:00')
  end
end

group_ids.each do |group_id|
  member_ids = conn.exec('select array_agg(member_id) as mids from users_groups where group_id = $1', [group_id]) do |result|
    result.check
    decoder = PG::TextDecoder::Array.new
    result.map { |row| decoder.decode(row['mids']).map(&:to_i) }.first
  end

  timestamps.call(window.next).each do |timestamp|
    member_ids.each do |mid|
      conn.exec_prepared('insert_messages',
                         [mid, group_id, Faker::Yoda.quote, timestamp], &:check)
    end
  end
end
