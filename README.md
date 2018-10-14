[scan files]
* ignore hidden files ("." files)

[mimetype/extension detection]
https://github.com/h2non/filetype
https://github.com/gabriel-vasile/mimetype

[lumberjack]
* fix go routine leaking

[doc fields]
-------- basic meta
* id (content-hash), keyword
* size, long
* file-name, keyword
* file-ext, keyword
* mime-type, keyword
* mime-subtype, keyword
* extension, keyword
-------- extend meta
* timestamp
* year
* month
* day
* hour
* minute
* second
* country
* city
