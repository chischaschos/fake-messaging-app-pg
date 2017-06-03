#!/usr/bin/env sh

app_name=fake_messaging_app

read -d '' -r command <<COMMAND
create table users ( id serial primary key, name text not null);
create table users_groups (group_id int not null, member_id int not null, unique(group_id, member_id));
create table messages (id serial primary key, user_id int not null, group_id int not null, message text, created_at timestamp default now());
COMMAND

dropdb --if-exists $app_name
createdb --owner postgres $app_name

psql --dbname $app_name --username postgres --command "$command"

bundle exec ruby fake_populate.rb

