require 'rubygems'
require 'bundler/setup'
require 'pg'
require 'faker'
require 'ostruct'
require 'byebug'

conn = PG.connect('dbname=fake_messaging_app')

# Create users
conn.prepare('insert_users', <<-SQL, &:check)
  INSERT INTO users (name)
  VALUES ($1)
SQL

1000.times do
  conn.exec_prepared('insert_users', [Faker::Name.name], &:check)
end

# Obtain a base user
user = conn.exec('SELECT * FROM users LIMIT 1') do |result|
  result.check
  OpenStruct.new(id: result.first['id'], name: result.first['name'])
end

puts "Our base user #{user}"

# Create groups for that user
conn.prepare('insert_users_groups', <<-SQL, &:check)
  INSERT INTO users_groups (group_id, member_id)
  VALUES ($1, $2)
SQL

# Let's create 20 groups, each group size will be exponential
20.times do |group_index|
  group_id = group_index + 1
  group_size = group_id**2

  random_members = conn.exec(<<-SQL, [user.id, group_size]) do |result|
    SELECT id
    FROM users
    WHERE id <> $1
    ORDER BY random()
    LIMIT $2
  SQL

    result.map { |row| row['id'] }
  end

  # insert our base user
  conn.exec_prepared('insert_users_groups', [group_id, user.id], &:check)
  random_members.each do |id|
    # now add extra users
    begin
      conn.exec_prepared('insert_users_groups', [group_id, id], &:check)
    rescue PG::UniqueViolation
    end
  end
end

# let's display the created group sizes
group_ids = conn.exec(<<-SQL) do |result|
  SELECT group_id AS g, count(*) AS c
  FROM users_groups
  GROUP BY 1
  ORDER BY 1
SQL

  result.map do |row|
    puts format('%i, %i', row['g'], row['c'])
    row['g']
  end
end

# now let's add messages
conn.prepare('insert_messages', <<-SQL, &:check)
  INSERT INTO messages (user_id, group_id, message, created_at)
  VALUES ($1, $2, $3, $4)
SQL

# All conversations mysteriously start this year on January the first
build_timestamps = lambda do |size|
  Array.new(size) { |index| Time.new(Time.now.year) + index }
end

group_ids.each do |group_id|
  member_ids = conn.exec(<<-SQL, [group_id]) do |result|
      SELECT ARRAY_AGG(member_id) AS mids
      FROM users_groups
      WHERE group_id = $1
    SQL

    result.check
    decoder = PG::TextDecoder::Array.new
    result.map { |row| decoder.decode(row['mids']).map(&:to_i) }.first
  end

  times_members = build_timestamps.call(member_ids.size).zip(member_ids)

  times_members.each do |timestamp, member_id|
    conn.exec_prepared('insert_messages',
                       [member_id, group_id, Faker::Yoda.quote, timestamp], &:check)
  end
end
