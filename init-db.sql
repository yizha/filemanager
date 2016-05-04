create database if not exists my_media_file default character set = 'utf8';

use my_media_file;

create table if not exists entry (

    id          int unsigned  not null auto_increment,
    path_md5    char(32)      not null,
    content_md5 char(32)      not null,
    path        varchar(4096) not null,
    size        int unsigned  not null,
    mime_type   varchar(64)   not null,
    status      tinyint(1)    not null default 0,

    primary key (id),
    unique  key k_entry_1 (path_md5),
    unique  key k_entry_2 (content_md5),
            key k_entry_3 (mime_type),
            key k_entry_4 (status)
);
