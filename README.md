# StreamSpider
Spider based on storm platform

## Environment
  - [Apache Storm](https://github.com/apache/storm)
  - [Redis](https://github.com/antirez/redis)
  - [Mongo](https://github.com/mongodb/mongo)

## Features

#### Incremental scrap && analyze

#### Define allowed URL patterns

#### Customize scrap strategy of certain pattern
  - limitation
  - patterns2follow
  - expire time

#### Update settings dynamically
  System will refetch settings after a certain time (cache), so it is possible to update settings dynamically.

## Topology
  There are one spout(`URLReader`) and five bolts in these topology. Bolts include `URLFilter`, `Downloader`, `HTMLParser`, `HTMLSaver`, `URLSaver`

#### URLReader:  Pop from redis waiting list to get urls

#### URLFilter:  Determine Which url will be downloaded.
 This bolt is the controller, in charge of :
 - Handle repeated urls
 - Pattern download count, ignore limitation exceeded pattern.

#### Downloader: Download url

#### HTMLParser: Parse urls from the page

#### HTMLSaver : Save page html to mongodb

#### URLSaver  : Push possible urls to redis waiting list


## Configuration
  There something  to (or can to be) configured
   - **urls_to_download** (Redis list, required ) : waiting list, absolute url path.
   - **allowed_url_patterns** (Redis sorted list, required, priority from highest score(5) to lowest score(1), zrevrangeBYScore): allowed url patterns to be downloaded
   - **url_pattern_setting_{pattern}** (Redis hash, optional) :
    - **limitation**: download count limitation in an interval
    - **frequency**: cache time, rescrap when expires
    - **patterns2follow**: which patterns to follow. In the form of pattern1,pattern2,..., default is .*


## TODO
   - [ ] proxy support
   - [ ] refetch after expires
   - [ ] more
