# StreamSpider
Spider based on storm platform

## Environment
  - [Apache Storm](https://github.com/apache/storm)
  - [Redis](https://github.com/antirez/redis)
  - [RabbitMQ](https://github.com/rabbitmq/rabbitmq-server)

## Features

#### Incremental scrap && analyze

#### Define allowed URL patterns

#### Customize scrap strategy of certain pattern
  - limitation
  - reset interval
  - expire time
  - parallelism

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

#### URLParser: Parse urls from the page

#### HTMLSaver : Save page html to MQ

#### URLSaver  : Push possible urls to redis waiting list


## Configuration
  There something  to (or can to be) configured

### **urls_to_download** (Redis list, required ) : waiting list, absolute url path.

### **allowed_url_patterns** (Redis sorted list, required, priority from highest score(5) to lowest score(1), zrevrangeBYScore): allowed url patterns to be downloaded

### **url_pattern_setting_{pattern}** (Redis hash, optional) : 
    - **limitation**: download count limitation in an interval
    - **interval**: duration to reset count
    - **expire**: cache time
    - **parallelism**: max number of workers working on this pattern(host)

## TODO
  - ignore nun-text pages (binary file)
  - consume faster