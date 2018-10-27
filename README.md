[scan files]
* ignore hidden files ("." files)

[mimetype]
file -p --mime -f [file-list-file]
file -p -f [file-list-file]

[exiftool to extract image/video file meta]
TODO

[lumberjack]
* fix go routine leaking

[doc fields]
-------- basic meta
* id (content-hash), keyword
* size, long
* file-name, keyword
* file-ext, keyword
* mime-desc, text/keyword
* mime-type, keyword
* mime-subtype, keyword
* mime-encoding, keyword
-------- extend meta
* timestamp
* year
* month
* day
* hour
* minute
* second
* location/country
* location/city
