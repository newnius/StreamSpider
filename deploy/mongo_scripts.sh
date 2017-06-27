use StreamSpider

db.createCollection("pages", {})

db.pages.drop()

db.pages.find({})

db.pages.find({ "url" : /oa.jlu.edu.cn/}, {url:1})

db.pages.count()

db.pages.renameCollection("pages_csdn")

db.pages.deleteMany({})


https://docs.mongodb.com/manual/tutorial/query-embedded-documents/
http://www.runoob.com/mongodb/mongodb-indexing.html


http://www.mongoing.com/archives/2797

db.pages.find({url: "http"}).explain("executionStats")

db.pages.ensureIndex({"url":"hashed"})
db.pages.ensureIndex({"url": 1})

## redis

llen urls_to_download

scard pending_urls_set



storm jar /mnt/StreamSpider-0.1.0-SNAPSHOT.jar com.newnius.streamspider.SpiderTopology


du -h --max-depth=1



8997861 assigned
8097229 downloaded

7675555 in queue


95G



1301354 hosts

86345 hosts




https://www.oschina.net/question/146430_122418
