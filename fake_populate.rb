require 'rubygems'
require 'bundler/setup'
require 'pg'
require 'faker'
require 'ostruct'

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

conn.exec('select group_id as g, count(*) as c from users_groups group by 1 order by 1') do |result|
  result.each do |row|
    puts format('%i, %i', row['g'], row['c'])
  end
end
