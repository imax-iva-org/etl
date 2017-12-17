create database if not exists api_data;

create user if not exists 'load_bot@localhost' identified by 'qwerty123';
grant select, insert, update, execute, show view on api_data.* to 'load_bot@localhost';
flush privileges;

create table if not exists api_data.fb (
	id varchar(50) not null,
    created_time datetime not null,
    link varchar(500) default null,
    name varchar(250) default null,
    permalink_url varchar(256) default null,
    shares int(6) unsigned default 0,
    status_type varchar(25) default null,
    type varchar(25) default null,
    actions int(6) unsigned default 0,
    source varchar(50) not null,
    update_time datetime not null
);

create or replace view generator_16 as
	select 0 as n
	union all
	select 1
	union all
	select 2
	union all
	select 3
	union all
	select 4
	union all
	select 5
	union all
	select 6
	union all
	select 7
	union all
	select 8
	union all
	select 9
	union all
	select 10
	union all
	select 11
	union all
	select 12
	union all
	select 13
	union all
	select 14
	union all
	select 15;

create or replace view generator_256 as
	select (hi.n * 16 + lo.n) as n
	from generator_16 as lo, generator_16 as hi;

create or replace view generator_4k as
	select ((hi.n << 8) | lo.n) as n
	from (generator_256 as lo
	join generator_16 as hi);

create or replace view generator_64k as
	select ((hi.n << 8) | lo.n) as n
    from generator_256 lo, generator_256 hi;


drop procedure if exists posts_count;
create procedure posts_count (days int)
begin

	select max(`update_time`) from api_data.fb into @max_date;
	set @start_date := date_add(@max_date, interval -days day);
	set @start_date_round := from_unixtime(floor(unix_timestamp(@start_date) / 600) * 600);
	set @interval_10min_num = floor(ifnull((unix_timestamp(@max_date) - unix_timestamp(@start_date_round)), 0) / 600+ 1);

	select
		intervals.sdate as dt,
		count(fb.id) as posts
	from (
		select
			date_add(@start_date_round, interval n * 10 minute) as sdate,
			date_add(@start_date_round, interval (n + 1) * 10 minute) as edate
		from generator_4k
		where n < @interval_10min_num
	) as intervals
	left join fb
		on fb.update_time >= intervals.sdate and fb.update_time < intervals.edate
	group by dt
	order by dt desc;
    
end;
