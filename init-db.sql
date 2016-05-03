drop database if exists my_media_file;
create database my_media_file default character set = 'utf8';

use my_media_file;

drop table if exists entry;

create table entry (
    id int unsigned not null auto_increment primary key,
    content_md5 char(32) not null,
    path_md5 char(32) not null,
    path varchar(4096) not null,
    size int unsigned not null,
    mime_type varchar(64) not null,
    mod_time datetime not null,
    key k_entry_1 (content_md5),
    unique key k_entry_2 (path_md5),
    key k_entry_3 (mime_type),
    key k_entry_4 (mod_time)
);
