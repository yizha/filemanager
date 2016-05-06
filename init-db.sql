create database if not exists my_file default character set = 'utf8';

use my_file;

create table if not exists file (

    id          int unsigned  not null auto_increment,
    path_md5    char(32)      not null,
    content_md5 char(32)      not null,
    path        varchar(4096) not null,
    path_prefix varchar(128)  not null,
    size        int unsigned  not null,
    mime_type   varchar(64)   not null,
    status      tinyint(1)    not null default 0,

    primary key (id),
    unique  key k_file_1 (path_md5),
            key k_file_2 (path_prefix),
    unique  key k_file_3 (content_md5),
            key k_file_4 (mime_type),
            key k_file_5 (status)
);
