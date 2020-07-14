# Data Set for TF-IDF based on English Wikipedia Dump

This data set is prepared based on [Wikipedia Dumps] using 
[Wikipedia Extractor] script from [TANL](http://medialab.di.unipi.it/wiki/Tanl) project.

[Wikipedia Dumps]: https://dumps.wikimedia.org/
[Wikipedia Extractor]: http://medialab.di.unipi.it/wiki/Wikipedia_Extractor
[Wikipedia Extractor (GitHub)]: https://github.com/attardi/wikiextractor
[TANL]: http://medialab.di.unipi.it/wiki/Tanl

Following command was used:

```bash
WikiExtractor/WikiExtractor.py -b 10M --filter_disambig_pages --discard_elements gallery,timeline,noinclude --ignored_tags abbr,b,big --no-templates --processes 4 -o enwiki-full-extracted/ enwiki-full-dump/enwiki-20190101-pages-articles.xml.bz2 >>enwiki-full-extracted/enwiki-full-extracted-output.log 2>&1
```

These files are based on the dump from `enwiki-20190101-pages-articles.xml.bz2`.
